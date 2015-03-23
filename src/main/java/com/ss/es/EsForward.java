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

//    private static final LinkedBlockingQueue<MessageObject> messages = new LinkedBlockingQueue<>();
//    private static final String TRACKID = "t";
//    private static TransportClient client = null;

    private static final String TRACKID_REG = "\"t\":\"\\d+";
    private static final String VISITOR_IDENTIFIER_REG = "\"vid\":\"[0-9a-zA-Z]+";

    private final TransportClient client;
    private final ConcurrentLinkedQueue<IndexRequest> requestQueue = new ConcurrentLinkedQueue<>();


    public EsForward() {
        this.client = getEsClient();
        init();
        handleRequest();
    }


//    public static void push(MessageObject httpMessage) {
//        messages.offer(httpMessage);
//    }

    private void init() {
//        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "es-cluster").put("client.transport.sniff", true).build();
//        client = new TransportClient(settings);
//        client.addTransportAddress(new InetSocketTransportAddress("192.168.1.120", 19300));

        int num = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(num);
//        TransportClient client = EsPools.getEsClient();
        for (int i = 0; i < num; i++) {
            executor.execute(() -> {
//                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                while (true) {
//                MessageObject message = null;
//                try {
//                    message = messages.take();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//                if (message == null) {
//                    continue;
//                }
//                String request = message.getHttpMessage().getUri();
//                if (!request.contains("?t=")) { // 判断是否包含track id
//                    continue;
//                }
//
//                QueryStringDecoder decoder = new QueryStringDecoder(request);
//                if (decoder.parameters().get(TRACKID) == null) {
//                    continue;
//                }
//
//                Map<String, Object> source = new HashMap<>();
//
//                source.putAll(message.getAttribute());
//                source.put("method", message.getHttpMessage().getMethod());
//                source.put("version", message.getHttpMessage().getProtocolVersion());
//                message.getHttpMessage().headers().entries().forEach(entry -> {
//                    source.put(entry.getKey(), entry.getValue());
//                });
//
//                decoder.parameters().forEach((k, v) -> {
//                    if (v.isEmpty())
//                        source.put(k, "");
//                    else
//                        source.put(k, v.get(0));
//                });

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
                                    String vid = matcher2.group().replace("\"vid\":\"", "");
                                    if (vidExists(vid)) {
                                        builder.setIndex("visitor-" + localDate.toString());
                                        EsOperator.push(builder.request());
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

//                    if (bulkRequestBuilder.numberOfActions() == 500) {
//                        bulkRequestBuilder.get();
//                        bulkRequestBuilder = client.prepareBulk();
//                    }
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
        return EsPools.getEsClient();
    }

}
