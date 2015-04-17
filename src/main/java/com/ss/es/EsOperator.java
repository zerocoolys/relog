package com.ss.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by baizz on 2015-3-20.
 */
public class EsOperator implements ElasticRequest {

    private static final ConcurrentLinkedQueue<Map<String, Object>> insertRequestQueue = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<Map<String, Object>> updateRequestQueue = new ConcurrentLinkedQueue<>();
    private final TransportClient client;


    public EsOperator(TransportClient client) {
        this.client = client;
        handleInsertRequest();
        handleUpdateRequest();
    }


    public void pushIndexRequest(Map<String, Object> request) {
        insertRequestQueue.offer(request);
    }

    public void pushUpdateRequest(Map<String, Object> request) {
        updateRequestQueue.offer(request);
    }

    private void handleInsertRequest() {
        Executors.newSingleThreadExecutor().execute(() -> {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                if (insertRequestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    bulkRequestBuilder.get();
                    bulkRequestBuilder = client.prepareBulk();
                } else if (!insertRequestQueue.isEmpty()) {
                    Map<String, Object> requestMap = insertRequestQueue.poll();

                    IndexRequestBuilder builder = client.prepareIndex();

                    try {
                        XContentBuilder contentBuilder = jsonBuilder().startObject();

                        // 设置field
                        for (Map.Entry<String, Object> entry : requestMap.entrySet()) {
                            if (ET.equals(entry.getKey())) {
                                continue;
                            }

                            contentBuilder.field(entry.getKey(), entry.getValue());
                        }

                        if (requestMap.containsKey(ET)) {

                            List<Map<String, String>> mapList = (ArrayList) requestMap.get(ET);

                            contentBuilder.startArray(ET);

                            for (Map<String, String> map : mapList) {
                                contentBuilder.startObject();
                                for (Map.Entry<String, String> entry : map.entrySet()) {
                                    contentBuilder.field(entry.getKey(), entry.getValue());
                                }
                                contentBuilder.endObject();
                            }

                            contentBuilder.endArray();
                        }
                        contentBuilder.endObject();


                        builder.setIndex(VISITOR_PREFIX + LocalDate.now().toString());
                        builder.setType(requestMap.get(T).toString());
                        builder.setSource(contentBuilder);


                        bulkRequestBuilder.add(builder.request());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (bulkRequestBuilder.numberOfActions() == EsPools.getBulkRequestNumber()) {
                        bulkRequestBuilder.get();
                        bulkRequestBuilder = client.prepareBulk();
                    }

                }

            }
        });
    }

    private void handleUpdateRequest() {
        Executors.newSingleThreadExecutor().execute(() -> {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                if (updateRequestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    bulkRequestBuilder.get();
                    bulkRequestBuilder = client.prepareBulk();
                } else if (!updateRequestQueue.isEmpty()) {
                    Map<String, Object> requestMap = updateRequestQueue.poll();

                    try {
                        XContentBuilder contentBuilder = jsonBuilder()
                                .startObject()
                                .field(CURR_ADDRESS, requestMap.get(CURR_ADDRESS))
                                .field(UNIX_TIME, requestMap.get(UNIX_TIME));

                        if (requestMap.containsKey(ET)) {
                            List<Map<String, String>> mapList = (ArrayList) requestMap.get(ET);

                            contentBuilder.startArray(ET);

                            for (Map<String, String> map : mapList) {
                                contentBuilder.startObject();
                                for (Map.Entry<String, String> entry : map.entrySet()) {
                                    contentBuilder.field(entry.getKey(), entry.getValue());
                                }
                                contentBuilder.endObject();
                            }

                            contentBuilder.endArray();
                        }
                        contentBuilder.endObject();

                        bulkRequestBuilder.add(client.prepareUpdate()
                                .setIndex(requestMap.get(INDEX).toString())
                                .setType(requestMap.get(TYPE).toString())
                                .setId(requestMap.get(ID).toString())
                                .setDoc(contentBuilder));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    if (bulkRequestBuilder.numberOfActions() == EsPools.getBulkRequestNumber()) {
                        bulkRequestBuilder.get();
                        bulkRequestBuilder = client.prepareBulk();
                    }

                }

            }
        });
    }

}
