package com.ss.es;

import com.alibaba.fastjson.JSON;
import com.ss.config.JRedisPools;
import com.ss.main.SearchEngineParser;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Sets;
import redis.clients.jedis.Jedis;

import java.io.UnsupportedEncodingException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yousheng on 15/3/16.
 */
public class EsForward implements ElasticRequest {

    // t -> TrackId; vid -> 访客唯一标识符； tt -> UV
//    private static final String TRACKID_REG = "\"t\":\"\\d+";
//    private static final String VISITOR_IDENTIFIER_REG = "\"tt\":\"[0-9a-zA-Z]+";
    private static final String ACCESS_PREFIX = "access-";
    private static final String VISITOR_PREFIX = "visitor-";

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
                            Map<String, Object> mapSource = (Map<String, Object>) JSON.parse(source);
                            String trackId = mapSource.get(T).toString();
                            String tt = mapSource.get(TT).toString();
                            try {
                                String refer = mapSource.get(RF).toString();
                                if ("-".equals(refer)) {
                                    mapSource.put(SE, null);
                                    mapSource.put(KW, null);
                                } else {
                                    String[] sk = SearchEngineParser.getSK(java.net.URLDecoder.decode(refer, "UTF-8"));
                                    mapSource.put(SE, sk[0]);
                                    mapSource.put(KW, sk[1]);
                                }
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }

                            LocalDate localDate = LocalDate.now();
                            builder.setType(trackId);
                            builder.setSource(mapSource);

                            Map<String, Object> doc = visitorExists(VISITOR_PREFIX + localDate.toString(), trackId, tt);
                            if (doc.isEmpty()) {
                                builder.setIndex(ACCESS_PREFIX + localDate.toString());
                                requestQueue.add(builder.request());

                                builder = getIndexRequestBuilder(VISITOR_PREFIX + localDate.toString(), trackId);

                                Set<String> currAddress = Sets.newHashSet(mapSource.remove(CURR_ADDRESS).toString());
                                Set<Long> utime = Sets.newHashSet(Long.valueOf(mapSource.remove(UNIX_TIME).toString()));
                                mapSource.put(CURR_ADDRESS, currAddress.toArray(new String[currAddress.size()]));
                                mapSource.put(UNIX_TIME, utime.toArray(new Long[utime.size()]));

                                builder.setSource(mapSource);
                                EsOperator.pushIndexRequest(builder.request());
                            } else {
                                builder.setIndex(ACCESS_PREFIX + localDate.toString());
                                requestQueue.add(builder.request());

                                builder = getIndexRequestBuilder(VISITOR_PREFIX + localDate.toString(), trackId);

                                List<String> currAddress = (ArrayList) doc.get(CURR_ADDRESS);
                                List<Long> utime = (ArrayList) doc.get(UNIX_TIME);

                                currAddress.add(mapSource.remove(CURR_ADDRESS).toString());
                                utime.add(Long.valueOf(mapSource.remove(UNIX_TIME).toString()));
                                mapSource.put(CURR_ADDRESS, currAddress.toArray(new String[currAddress.size()]));
                                mapSource.put(UNIX_TIME, utime.toArray(new Long[utime.size()]));
                                EsOperator.pushUpdateRequest(doc);
                            }

//                            Matcher matcher1 = Pattern.compile(TRACKID_REG).matcher(source);
//                            if (matcher1.find()) {
//                                String trackId = matcher1.group().replace("\"t\":\"", "");
//
//                                LocalDate localDate = LocalDate.now();
//                                builder.setType(trackId);
//                                builder.setSource(source);
//
//                                Matcher matcher2 = Pattern.compile(VISITOR_IDENTIFIER_REG).matcher(source);
//                                if (matcher2.find()) {
//                                    String tt = matcher2.group().replace("\"tt\":\"", "");
//                                    if (visitorExists(VISITOR_PREFIX + localDate.toString(), trackId, tt)) {
//                                        builder.setIndex(ACCESS_PREFIX + localDate.toString());
//                                        requestQueue.add(builder.request());
//                                    } else {
//                                        builder.setIndex(ACCESS_PREFIX + localDate.toString());
//                                        requestQueue.add(builder.request());
//
//                                        builder = getIndexRequestBuilder(VISITOR_PREFIX + localDate.toString(), trackId);
//                                        builder.setSource(source);
//                                        EsOperator.push(builder.request());
//                                    }
//                                }
//                            }
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
