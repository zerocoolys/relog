package com.ss.es;

import com.alibaba.fastjson.JSON;
import com.ss.main.Constants;
import com.ss.parser.KeywordExtractor;
import com.ss.parser.SearchEngineParser;
import com.ss.utils.UrlUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Lists;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Created by yousheng on 15/3/16.
 */
public class EsForward implements Constants {

    private final BlockingQueue<Map<String, Object>> queue = new LinkedBlockingQueue<>();

    public EsForward(TransportClient client) {
        BlockingQueue<IndexRequest> requestQueue = new LinkedBlockingQueue<>();
//        EsOperator esOperator = new EsOperator(client);
//        preHandle(client, requestQueue, esOperator);
        preHandle(client, requestQueue);
        handleRequest(client, requestQueue);
    }

    public void add(Map<String, Object> obj) {
        queue.add(obj);
    }

    private void preHandle(TransportClient client, BlockingQueue<IndexRequest> requestQueue) {
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
                        _location = url.getProtocol() + DOUBLE_SLASH + url.getHost().split("/")[0];
                        mapSource.put(CURR_ADDRESS, _location);
                        mapSource.put(DESTINATION_URL, hasPromotion ? _location : PLACEHOLDER);
                    } catch (MalformedURLException e) {
                        e.printStackTrace();
                    }
                } else {
                    mapSource.put(ENTRANCE, 0);
                }


                String trackId = mapSource.get(T).toString();
                String tt = mapSource.get(TT).toString();   // 访问次数标识符
                mapSource.put(TT, tt);

                try {
                    String refer = mapSource.get(RF).toString();
                    // 来源类型解析
                    if (PLACEHOLDER.equals(refer)) {  // 直接访问
                        mapSource.put(SE, PLACEHOLDER);
                        mapSource.put(KW, PLACEHOLDER);
                        mapSource.put(RF_TYPE, VAL_RF_TYPE_DIRECT);
                        mapSource.put(DOMAIN, PLACEHOLDER);
                    } else {
                        List<String> skList = Lists.newArrayList();
                        boolean found = SearchEngineParser.getSK(java.net.URLDecoder.decode(refer, StandardCharsets.UTF_8.name()), skList);
                        // extract domain from rf
                        URL url = new URL(refer);
                        mapSource.put(DOMAIN, url.getProtocol() + DOUBLE_SLASH + url.getHost());
                        if (found) {
                            mapSource.put(SE, skList.remove(0));
                            mapSource.put(KW, skList.remove(0));
                            mapSource.put(RF_TYPE, VAL_RF_TYPE_SE);
                        } else {
                            String rfHost = url.getHost();
                            URL currLocUrl = new URL(mapSource.get(CURR_ADDRESS).toString());
                            if (rfHost.equals(currLocUrl.getHost()))
                                mapSource.put(RF_TYPE, VAL_RF_TYPE_SITES);
                            else
                                mapSource.put(RF_TYPE, VAL_RF_TYPE_OUTLINK);
                        }
                    }

                    // 访问URL路径解析
                    String location = UrlUtils.removeProtocol(mapSource.get(CURR_ADDRESS).toString());
                    if (location.contains(QUESTION_MARK))
                        location = location.substring(0, location.indexOf(QUESTION_MARK));

                    Map<String, String> pathMap = new HashMap<>();

                    final AtomicInteger integer = new AtomicInteger(0);
                    Consumer<String> pathConsumer = (String c) -> pathMap.put(HTTP_PATH + (integer.getAndIncrement()), c);

                    Arrays.asList(location.split("/")).stream().filter((p) -> !p.isEmpty() || !p.startsWith(HTTP_PREFIX)).forEach(pathConsumer);

                    mapSource.put(PATHS, pathMap);
                } catch (NullPointerException | UnsupportedEncodingException | MalformedURLException e) {
                    e.printStackTrace();
                }

                // 事件跟踪处理
                String eventInfo = mapSource.getOrDefault(ET, EMPTY_STRING).toString();
                if (!eventInfo.isEmpty()) {
                    String[] eventArr = mapSource.get(ET).toString().split("\\*");
                    Map<String, Object> etJsonMap = new HashMap<>();
                    etJsonMap.put(ET_CATEGORY, eventArr[0]);
                    etJsonMap.put(ET_ACTION, eventArr[1]);
                    etJsonMap.put(ET_LABEL, eventArr[2]);
                    etJsonMap.put(ET_VALUE, eventArr.length == 3 ? EMPTY_STRING : eventArr[3]);
                    mapSource.put(ET, JSON.toJSONString(etJsonMap));
                    mapSource.put(ENTRANCE, -1);
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
