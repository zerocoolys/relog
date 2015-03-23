package com.ss.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

/**
 * Created by baizz on 2015-3-20.
 * <p/>
 * elasticsearch数据更新操作类
 */
public class EsOperator implements ElasticRequest {

    private static final ConcurrentLinkedQueue<IndexRequest> requestQueue = new ConcurrentLinkedQueue<>();

    private final TransportClient client;


    public EsOperator() {
        this.client = getEsClient();
        init();
    }


    public static void push(IndexRequest request) {
        requestQueue.offer(request);
    }

    private void init() {
//        TransportClient client = EsPools.getEsClient();
        Executors.newSingleThreadExecutor().execute(() -> {
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                if (requestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    bulkRequestBuilder.get();
                    bulkRequestBuilder = client.prepareBulk();
                } else if (!requestQueue.isEmpty()) {
                    IndexRequest request = requestQueue.poll();
                    bulkRequestBuilder.add(request);

                    if (bulkRequestBuilder.numberOfActions() == 1_000) {
                        bulkRequestBuilder.get();
                        bulkRequestBuilder = client.prepareBulk();
                    }

                }

            }
        });

    }

    @Override
    public TransportClient getEsClient() {
        return EsPools.getEsClient();
    }
}
