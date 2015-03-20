package com.ss.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;

import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;

/**
 * Created by baizz on 2015-3-20.
 * <p/>
 * elasticsearch数据更新操作类
 */
public class EsOperator {

    private static final ConcurrentLinkedQueue<Map<String, Object>> requestQueue = new ConcurrentLinkedQueue<>();


    public EsOperator() {
        init();
    }


    public static void push(Map<String, Object> requestMessage) {
        requestQueue.offer(requestMessage);
    }

    private void init() {
        TransportClient client = EsPools.getEsClient();
        Executors.newSingleThreadExecutor().execute(() -> {
//            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
//            while (true) {
//                if (requestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
//                    bulkRequestBuilder.get();
//                    bulkRequestBuilder = client.prepareBulk();
//                } else if (!requestQueue.isEmpty()) {
//                    Map<String, Object> source = requestQueue.poll();
//                    IndexRequestBuilder builder = client.prepareIndex();
//                    LocalDate localDate = LocalDate.now();
//                    builder.setIndex("visitor-" + localDate.getYear() + "-" + localDate.getMonthValue() + "-" + localDate.getDayOfMonth());
//                    builder.setType(source.get("t").toString());
//                    builder.setSource(source);
//                    bulkRequestBuilder.add(builder.request());
//
//                    if (bulkRequestBuilder.numberOfActions() == 1_000) {
//                        bulkRequestBuilder.get();
//                        bulkRequestBuilder = client.prepareBulk();
//                    }
//
//                }
//
//            }
        });

    }
}
