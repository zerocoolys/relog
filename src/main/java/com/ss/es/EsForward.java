package com.ss.es;

import com.ss.config.JRedisPools;
import com.ss.main.Constants;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import redis.clients.jedis.Jedis;

import java.time.LocalDate;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yousheng on 15/3/16.
 */
public class EsForward implements ElasticRequest, Constants {

    // vid -> 访客唯一标识符； tt -> UV
    private static final String TRACKID_REG = "\"t\":\"\\d+";
    private static final String VISITOR_IDENTIFIER_REG = "\"tt\":\"[0-9a-zA-Z]+";

    private final TransportClient client;
    private final ConcurrentLinkedQueue<IndexRequest> requestQueue = new ConcurrentLinkedQueue<>(); // PV


    public EsForward() {
        this.client = getEsClient();
        init();
        handleRequest();
    }


    private void init() {
        int num = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(num);
        for (int i = 0; i < num; i++) {
            executor.execute(() -> {

                while (true) {

                    IndexRequestBuilder builder = client.prepareIndex();

                    Jedis jedis = null;
                    try {
                        jedis = JRedisPools.getConnection();
                        String source = jedis.rpop(ACCESS_MESSAGE);
                        if (source != null) {
                            Matcher matcher1 = Pattern.compile(TRACKID_REG).matcher(source);
                            if (matcher1.find()) {
                                String trackId = matcher1.group().replace("\"t\":\"", "");

                                LocalDate localDate = LocalDate.now();
                                builder.setType(trackId);
                                builder.setSource(source);

                                Matcher matcher2 = Pattern.compile(VISITOR_IDENTIFIER_REG).matcher(source);
                                if (matcher2.find()) {
                                    String tt = matcher2.group().replace("\"tt\":\"", "");
                                    if (visitorExists("access-2015-03-23", trackId, tt)) {
                                        builder.setIndex("access-" + localDate.toString());
                                        requestQueue.add(builder.request());
                                    } else {
                                        builder.setIndex("access-" + localDate.toString());
//                                        bulkRequestBuilder.add(builder.request());
                                        requestQueue.add(builder.request());

                                        builder = getIndexRequestBuilder("visitor-" + localDate.toString(), trackId);
                                        builder.setSource(source);
                                        EsOperator.push(builder.request());
                                    }
                                }
                            }
                        }
                    } finally {
                        JRedisPools.returnJedis(jedis);
                    }

                }
            });
        }

    }

    private void handleRequest() {
        Executors.newSingleThreadExecutor().execute(() -> {
            TransportClient client = EsPools.getEsClient();
            BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
            while (true) {
                if (requestQueue.isEmpty() && bulkRequestBuilder.numberOfActions() > 0) {
                    bulkRequestBuilder.get();
                    bulkRequestBuilder = client.prepareBulk();
                } else if (!requestQueue.isEmpty()) {
                    bulkRequestBuilder.add(requestQueue.poll());
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
        if (client == null) {
            synchronized (this) {
                if (client == null)
                    return EsPools.getEsClient();
            }
        }
        return client;
    }

}
