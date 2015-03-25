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

    private static final ConcurrentLinkedQueue<IndexRequest> requestQueue = new ConcurrentLinkedQueue<>();  // UV
    private static final ConcurrentLinkedQueue<Map<String, Object>> updateRequestQueue = new ConcurrentLinkedQueue<>();

    private final TransportClient client;


    public EsOperator() {
        this.client = getEsClient();
        init();
    }


    public static void pushIndexRequest(IndexRequest request) {
        requestQueue.offer(request);
    }

    public static void pushUpdateRequest(Map<String, Object> request) {
        updateRequestQueue.offer(request);
    }

    private void init() {
        Executors.newSingleThreadExecutor().execute(() -> {
            BulkRequestBuilder bulkRequestBuilder = getBulkRequestBuilder();
            while (true) {
                if (requestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    bulkRequestBuilder.get();
                    bulkRequestBuilder = getBulkRequestBuilder();
                } else if (!requestQueue.isEmpty()) {
                    IndexRequest request = requestQueue.poll();
                    bulkRequestBuilder.add(request);

                    if (bulkRequestBuilder.numberOfActions() == 1_000) {
                        bulkRequestBuilder.get();
                        bulkRequestBuilder = getBulkRequestBuilder();
                    }

                }

            }
        });

        Executors.newSingleThreadExecutor().execute(() -> {
            BulkRequestBuilder bulkRequestBuilder = getBulkRequestBuilder();
            while (true) {
                if (updateRequestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    bulkRequestBuilder.get();
                    bulkRequestBuilder = getBulkRequestBuilder();
                } else if (!updateRequestQueue.isEmpty()) {
                    Map<String, Object> requestMap = updateRequestQueue.poll();

                    try {
                        bulkRequestBuilder.add(getUpdateRequestBuilder()
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

                    if (bulkRequestBuilder.numberOfActions() == 1_000) {
                        bulkRequestBuilder.get();
                        bulkRequestBuilder = getBulkRequestBuilder();
                    }

                }

            }
        });

    }

    @Override
    public TransportClient getEsClient() {
        if (client == null) {
            synchronized (this) {
                if (client == null)
                    return EsPools.getEsClient();
            }
        }
        return client;
    }
}
