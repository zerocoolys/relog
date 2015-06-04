package com.ss.es;

import com.ss.parser.KeywordExtractor;
import com.ss.parser.SearchEngineParser;
import com.ss.utils.UrlUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Created by yousheng on 15/3/16.
 */
@SuppressWarnings("unchecked")
public class EsForward implements ElasticRequest {

    private BlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>();

    public EsForward(TransportClient client) {
        BlockingQueue<IndexRequest> requestQueue = new LinkedBlockingQueue<>();
        EsOperator esOperator = new EsOperator(client);
        preHandle(client, requestQueue, esOperator);
        handleRequest(client, requestQueue);
    }

    public void add(Map<String, Object> obj) {
        queue.add(obj);
    }

    private void preHandle(TransportClient client, BlockingQueue<IndexRequest> requestQueue, EsOperator esOperator) {
        Thread t = new Thread(() -> {
            while (true) {
                Map<String, Object> mapSource = null;
                try {
                    mapSource = queue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (mapSource == null || !mapSource.containsKey(T) || !mapSource.containsKey(TT))
                    continue;

                // 检测是否是一次的新的访问(1->新的访问, 0->同一次访问)
                int identifier = Integer.valueOf(mapSource.getOrDefault(NEW_VISIT, 0).toString());
                if (identifier == 1) {
                    mapSource.put(ENTRANCE, 1);
                    mapSource.remove(NEW_VISIT);
                    String _location = mapSource.get(CURR_ADDRESS).toString();

                    boolean hasPromotion = false;
                    if (_location.contains(SEM_KEYWORD_IDENTIFIER)) {
                        // keyword extract
                        Map<String, Object> keywordInfoMap = KeywordExtractor.parse(_location);
                        if (!keywordInfoMap.isEmpty())
                            mapSource.putAll(keywordInfoMap);

                        hasPromotion = true;
                    }

                    try {
                        URL url = new URL(_location);
                        _location = url.getProtocol() + "://" + url.getHost().split("/")[0];
                        mapSource.put(CURR_ADDRESS, _location);
                        mapSource.put(DESTINATION_URL, hasPromotion ? _location : DELIMITER);
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    }
                } else {
                    mapSource.put(ENTRANCE, 0);
                }


                String trackId = mapSource.get(T).toString();   // 对应于elasticsearch的type
                String tt = mapSource.get(TT).toString();   // 访问次数标识符
                mapSource.put(TT, tt);

                try {
                    String refer = mapSource.get(RF).toString();
                    // 来源类型解析
                    if (DELIMITER.equals(refer)) {  // 直接访问
                        mapSource.put(SE, DELIMITER);
                        mapSource.put(KW, DELIMITER);
                        mapSource.put(RF_TYPE, 1);
                        mapSource.put(DOMAIN, DELIMITER);
                    } else {
                        String[] sk = SearchEngineParser.getSK(java.net.URLDecoder.decode(refer, StandardCharsets.UTF_8.name()));
                        // extract domain from rf
                        URL url = new URL(refer);
                        mapSource.put(DOMAIN, url.getProtocol() + "://" + url.getHost());
                        if (DELIMITER.equals(sk[0]) && DELIMITER.equals(sk[1]))
                            mapSource.put(RF_TYPE, 3);
                        else {
                            mapSource.put(SE, sk[0]);
                            mapSource.put(KW, sk[1]);
                            mapSource.put(RF_TYPE, 2);
                        }
                    }

                    // 访问URL路径解析
                    String location = UrlUtils.removeProtocol(mapSource.get(CURR_ADDRESS).toString());
                    if (location.contains("?"))
                        location = location.substring(0, location.indexOf("?"));

                    Map<String, String> pathMap = new HashMap<>();

                    final AtomicInteger integer = new AtomicInteger(0);
                    Consumer<String> pathConsumer = (String c) -> pathMap.put("path" + (integer.getAndIncrement()), c);

                    Arrays.asList(location.split("/")).stream().filter((p) -> {
                        return !p.isEmpty() || !p.startsWith("http:");
                    }).forEach(pathConsumer);

                    mapSource.put(PATHS, pathMap);
                } catch (NullPointerException | UnsupportedEncodingException | MalformedURLException e) {
                    e.printStackTrace();
                }

//                LocalDate localDate = LocalDate.now();
//                Map<String, Object> tmpMapSource = new HashMap<>(mapSource);
//                try {
//                    String _loc = tmpMapSource.get(CURR_ADDRESS).toString();
//                    if (_loc.contains(SEM_KEYWORD_IDENTIFIER)) {
//                        URL url = new URL(_loc);
//                        tmpMapSource.put(CURR_ADDRESS, url.getProtocol() + "://" + url.getHost());
//                    }
//                } catch (MalformedURLException e) {
//                    e.printStackTrace();
//                }

//                Map<String, Object> doc = visitorExists(client.prepareSearch(), VISITOR_PREFIX + localDate.toString(), trackId, tt);
                IndexRequestBuilder builder = client.prepareIndex();
                builder.setIndex(ACCESS_PREFIX + LocalDate.now().toString());
                builder.setType(trackId);
                builder.setSource(mapSource);
                requestQueue.add(builder.request());


                // TODO 事件转化统计信息处理, migrate to access
//                String eventAttr = mapSource.getOrDefault(ET, "").toString();
//                Map<String, String> eventMap = new LinkedHashMap<>();
//
//                if (eventAttr.isEmpty()) {
//                    if (doc.isEmpty())  // 入口页面
//                        tmpMapSource.put(ENTRANCE, 1);
//                    else
//                        tmpMapSource.put(ENTRANCE, 0);
//
//                    builder.setSource(tmpMapSource);
//                    requestQueue.add(builder.request());
//                } else {
//                    String[] eventArr = eventAttr.split("\\*");
//                    eventMap.put(ET_CATEGORY, eventArr[0]);
//                    eventMap.put(ET_ACTION, eventArr[1]);
//                    eventMap.put(ET_LABEL, eventArr[2]);
//                    eventMap.put(ET_VALUE, eventArr.length == 3 ? "" : eventArr[3]);
//                }
//
//                mapSource.remove(PATHS);
//                if (doc.isEmpty()) {    // 一次新的访问
//                    builder = client.prepareIndex(VISITOR_PREFIX + localDate.toString(), trackId);
//
//                    Set<String> currAddress = Sets.newHashSet(mapSource.remove(CURR_ADDRESS).toString());
//                    Set<Long> utime = Sets.newHashSet(Long.valueOf(mapSource.remove(UNIX_TIME).toString()));
//                    mapSource.put(CURR_ADDRESS, currAddress.toArray(new String[currAddress.size()]));
//                    mapSource.put(UNIX_TIME, utime.toArray(new Long[utime.size()]));
//                    builder.setSource(mapSource);
//
//                    if (!eventMap.isEmpty()) {
//                        List<Map<String, String>> events = new ArrayList<>();
//                        events.add(eventMap);
//                        mapSource.put(ET, events);
//                    }
//
//                    esOperator.pushIndexRequest(mapSource);
//                } else {    // 同一次访问
//                    builder = client.prepareIndex(VISITOR_PREFIX + localDate.toString(), trackId);
//
//                    List<String> currAddress = (ArrayList<String>) doc.get(CURR_ADDRESS);
//                    List<Long> utime = (ArrayList<Long>) doc.get(UNIX_TIME);
//
//                    if (doc.containsKey(ET) && !eventMap.isEmpty()) {
//                        List<Map<String, String>> events = (ArrayList) doc.get(ET);
//                        events.add((eventMap));
//                        doc.put(ET, events);
//                    } else if (!doc.containsKey(ET) && !eventMap.isEmpty()) {
//                        List<Map<String, String>> events = new ArrayList<>();
//                        events.add(eventMap);
//                        doc.put(ET, events);
//                    }
//
//                    currAddress.add(mapSource.remove(CURR_ADDRESS).toString());
//                    utime.add(Long.valueOf(mapSource.remove(UNIX_TIME).toString()));
//                    mapSource.put(CURR_ADDRESS, currAddress.toArray(new String[currAddress.size()]));
//                    mapSource.put(UNIX_TIME, utime.toArray(new Long[utime.size()]));
//                    esOperator.pushUpdateRequest(doc);
//                }
            }

        });

        t.setName("request-preHandle");
        t.start();

    }

    private void handleRequest(TransportClient client, BlockingQueue<IndexRequest> requestQueue) {
        Thread t = new Thread(() -> {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                IndexRequest request = null;
                try {
                    request = requestQueue.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (request == null)
                    continue;

                bulkRequestBuilder.add(request);

                if (requestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    submitRequest(bulkRequestBuilder);
                    bulkRequestBuilder = client.prepareBulk();
                    continue;
                }

                if (bulkRequestBuilder.numberOfActions() == EsPools.getBulkRequestNumber()) {
                    submitRequest(bulkRequestBuilder);
                    bulkRequestBuilder = client.prepareBulk();
                }

            }
        });
        t.setName("handleAccessInsert");
        t.start();
    }

    private void submitRequest(BulkRequestBuilder bulkRequestBuilder) {
        BulkResponse responses = bulkRequestBuilder.get();
        if (responses.hasFailures()) {
            System.out.println("Failure: " + responses.buildFailureMessage());
        }
    }

}
