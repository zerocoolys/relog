package com.ss.main;

import com.ss.config.JRedisPools;
import com.ss.vo.MessageObject;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import redis.clients.jedis.Jedis;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by yousheng on 15/3/16.
 */
public class EsForward {

    private static final LinkedBlockingQueue<MessageObject> messages = new LinkedBlockingQueue<>();
    private static final String TRACKID = "t";
    private static final String TRACKID_REG = "\"t\":\"\\d+";

    private static TransportClient client = null;
    private static Map<String, String> esMap = new HashMap<>();

    static {
        if (client == null) {
            synchronized (EsForward.class) {
                if (client == null) {
                    client = initEsClient();
                }
            }
        }
    }

    public EsForward() {
        init();
    }


//    public static void push(MessageObject httpMessage) {
//        messages.offer(httpMessage);
//    }

    private void init() {


//        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "es-cluster").put("client.transport.sniff", true).build();
//
//        client = new TransportClient(settings);
//
//        client.addTransportAddress(new InetSocketTransportAddress("192.168.1.120", 19300));


        int num = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(num);
        for (int i = 0; i < num; i++) {
            executor.execute(() -> {
                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

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
                        String source = jedis.rpop(RedisForward.ACCESS_MESSAGE);
                        if (source != null) {
                            Matcher matcher = Pattern.compile(TRACKID_REG).matcher(source);
                            if (matcher.find()) {
                                String trackId = matcher.group().replace("\"t\":\"", "");

                                Calendar calendar = Calendar.getInstance();
                                builder.setIndex("access-" + calendar.get(Calendar.YEAR) + "-" + (calendar.get(Calendar.MONTH) + 1) + "-" + calendar.get(Calendar.DATE));
                                builder.setType(trackId);
                                builder.setSource(source);

                                bulkRequestBuilder.add(builder.request());
                            }
                        }
                    } finally {
                        JRedisPools.returnJedis(jedis);
                    }

                    if (bulkRequestBuilder.numberOfActions() == 500) {
                        bulkRequestBuilder.get();
                        bulkRequestBuilder = client.prepareBulk();
                    }
                }
            });
        }

//        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()).execute(() -> {
//
//        });

    }

    private static TransportClient initEsClient() {
        TransportClient client = null;
        try {
            ResourceBundle bundle = ResourceBundle.getBundle("elasticsearch");
            String[] hosts = bundle.getString("es.host").split(",");
            List<InetSocketTransportAddress> addressList = new ArrayList<>();
            for (String host : hosts) {
                String[] arr = host.split(":");
                if (arr.length == 1)
                    addressList.add(new InetSocketTransportAddress(arr[0], 19300));
                else if (arr.length == 2)
                    addressList.add(new InetSocketTransportAddress(arr[0], Integer.valueOf(arr[1])));

            }
            String clusterName = bundle.getString("es.cluster");

            //设置client.transport.sniff为true来使客户端去嗅探整个集群的状态, 把集群中其它机器的ip地址加到客户端中
            Settings settings = ImmutableSettings.settingsBuilder().put(esMap).put("cluster.name", clusterName).put("client.transport.sniff", true).build();
            Class<?> clazz = Class.forName(TransportClient.class.getName());
            Constructor<?> constructor = clazz.getDeclaredConstructor(Settings.class);
            constructor.setAccessible(true);
            client = (TransportClient) constructor.newInstance(settings);
            client.addTransportAddresses(addressList.toArray(new InetSocketTransportAddress[addressList.size()]));
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return client;
    }

}
