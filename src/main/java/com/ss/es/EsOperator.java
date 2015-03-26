package com.ss.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by baizz on 2015-3-20.
 */
public class EsOperator implements ElasticRequest {

    private static final ConcurrentLinkedQueue<IndexRequest> insertRequestQueue = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<Map<String, Object>> updateRequestQueue = new ConcurrentLinkedQueue<>();
    private final TransportClient client;


    public EsOperator(TransportClient client) {
        this.client = client;
        handleInsertRequest();
        handleUpdateRequest();
    }


    public void pushIndexRequest(IndexRequest request) {
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
                    IndexRequest request = insertRequestQueue.poll();
                    bulkRequestBuilder.add(request);

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
                        bulkRequestBuilder.add(client.prepareUpdate()
                                .setIndex(requestMap.get(INDEX).toString())
                                .setType(requestMap.get(TYPE).toString())
                                .setId(requestMap.get(ID).toString())
                                .setDoc(jsonBuilder()
                                        .startObject()
                                        .field(CURR_ADDRESS, requestMap.get(CURR_ADDRESS))
                                        .field(UNIX_TIME, requestMap.get(UNIX_TIME))
                                        .endObject()));
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
