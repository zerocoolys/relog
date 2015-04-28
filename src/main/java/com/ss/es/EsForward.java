package com.ss.es;

import com.ss.main.SearchEngineParser;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Sets;

import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

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

                IndexRequestBuilder builder = client.prepareIndex();

                String trackId = mapSource.get(T).toString();
                String tt = mapSource.get(TT).toString();
                try {
                    String refer = mapSource.get(RF).toString();
                    if (DELIMITER.equals(refer)) {
                        mapSource.put(SE, DELIMITER);
                        mapSource.put(KW, DELIMITER);
                        mapSource.put(RF_TYPE, 1);
                    } else {
                        String[] sk = SearchEngineParser.getSK(java.net.URLDecoder.decode(refer, "UTF-8"));
                        mapSource.put(SE, sk[0]);
                        mapSource.put(KW, sk[1]);
                        if (DELIMITER.equals(sk[0]) && DELIMITER.equals(sk[1]))
                            mapSource.put(RF_TYPE, 3);
                        else
                            mapSource.put(RF_TYPE, 2);
                    }
                } catch (NullPointerException | UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

                LocalDate localDate = LocalDate.now();
                builder.setType(trackId);
                builder.setSource(mapSource);

                Map<String, Object> doc = visitorExists(client.prepareSearch(), VISITOR_PREFIX + localDate.toString(), trackId, tt);
                builder.setIndex(ACCESS_PREFIX + localDate.toString());


                // 事件转化统计信息处理
                String eventAttr = mapSource.getOrDefault(ET, "").toString();
                Map<String, String> eventMap = new LinkedHashMap<>();


                if (eventAttr.isEmpty())
                    requestQueue.add(builder.request());
                else {
                    String[] eventArr = eventAttr.split("\\*");
                    eventMap.put(ET_CATEGORY, eventArr[0]);
                    eventMap.put(ET_ACTION, eventArr[1]);
                    eventMap.put(ET_LABEL, eventArr[2]);
                    eventMap.put(ET_VALUE, eventArr.length == 3 ? "" : eventArr[3]);
                }


                if (doc.isEmpty()) {
                    builder = client.prepareIndex(VISITOR_PREFIX + localDate.toString(), trackId);

                    Set<String> currAddress = Sets.newHashSet(mapSource.remove(CURR_ADDRESS).toString());
                    Set<Long> utime = Sets.newHashSet(Long.valueOf(mapSource.remove(UNIX_TIME).toString()));
                    mapSource.put(CURR_ADDRESS, currAddress.toArray(new String[currAddress.size()]));
                    mapSource.put(UNIX_TIME, utime.toArray(new Long[utime.size()]));
                    builder.setSource(mapSource);


                    if (!eventMap.isEmpty()) {
                        List<Map<String, String>> events = new ArrayList<>();
                        events.add(eventMap);
                        mapSource.put(ET, events);
                    }

                    esOperator.pushIndexRequest(mapSource);
                } else {
                    builder = client.prepareIndex(VISITOR_PREFIX + localDate.toString(), trackId);

                    List<String> currAddress = (ArrayList) doc.get(CURR_ADDRESS);
                    List<Long> utime = (ArrayList) doc.get(UNIX_TIME);

                    if (doc.containsKey(ET) && !eventMap.isEmpty()) {
                        List<Map<String, String>> events = (ArrayList) doc.get(ET);
                        events.add((eventMap));
                        doc.put(ET, events);
                    } else if (!doc.containsKey(ET) && !eventMap.isEmpty()) {
                        List<Map<String, String>> events = new ArrayList<>();
                        events.add(eventMap);
                        doc.put(ET, events);
                    }

                    currAddress.add(mapSource.remove(CURR_ADDRESS).toString());
                    utime.add(Long.valueOf(mapSource.remove(UNIX_TIME).toString()));
                    mapSource.put(CURR_ADDRESS, currAddress.toArray(new String[currAddress.size()]));
                    mapSource.put(UNIX_TIME, utime.toArray(new Long[utime.size()]));
                    esOperator.pushUpdateRequest(doc);
                }
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
                if (bulkRequestBuilder.numberOfActions() == EsPools.getBulkRequestNumber()) {
                    bulkRequestBuilder.get();
                    bulkRequestBuilder = client.prepareBulk();
                }

            }
        });
        t.setName("handleAccessInsert");
        t.start();
    }

}
