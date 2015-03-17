package com.ss.main;

import com.ss.vo.MessageObject;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by yousheng on 15/3/16.
 */
public class EsForward {

    TransportClient client;

    private static final LinkedBlockingQueue<MessageObject> messages = new LinkedBlockingQueue<>();

    private static final String TRACKID = "t";

    public EsForward() {
        init();
    }


    public static void push(MessageObject httpMessage) {
        messages.offer(httpMessage);
    }

    private void init() {


        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "es-cluster").put("client.transport.sniff", true).build();

        client = new TransportClient(settings);

        client.addTransportAddress(new InetSocketTransportAddress("192.168.1.120", 19300));


        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {

                BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

                while (true) {
                    MessageObject message = null;
                    try {
                        message = messages.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (message == null) {
                        continue;
                    }
                    String request = message.getHttpMessage().getUri();
                    if (!request.contains("?t=")) { // 判断是否包含track id
                        continue;
                    }

                    QueryStringDecoder decoder = new QueryStringDecoder(request);
                    if (decoder.parameters().get(TRACKID) == null) {
                        continue;
                    }

                    Map<String, Object> source = new HashMap<>();

                    source.putAll(message.getAttribute());
                    source.put("method", message.getHttpMessage().getMethod());
                    source.put("version", message.getHttpMessage().getProtocolVersion());
                    message.getHttpMessage().headers().entries().forEach(entry -> {
                        source.put(entry.getKey(), entry.getValue());
                    });

                    decoder.parameters().forEach((k, v) -> {
                        if (v.isEmpty())
                            source.put(k, "");
                        else
                            source.put(k, v.get(0));
                    });

                    Calendar calendar = Calendar.getInstance();

                    IndexRequestBuilder builder = client.prepareIndex();
                    builder.setIndex("access-" + calendar.get(Calendar.YEAR) + "-" + calendar.get(Calendar.MONTH) + "-" + calendar.get(Calendar.DATE));
                    builder.setType(source.get(TRACKID).toString());
                    builder.setSource(source);

                    bulkRequestBuilder.add(builder.request());

                    if (bulkRequestBuilder.numberOfActions() == 500) {
                        bulkRequestBuilder.get();
                        bulkRequestBuilder = client.prepareBulk();
                    }
                }
            }
        });

    }


}
