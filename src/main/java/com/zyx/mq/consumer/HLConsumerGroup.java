package com.zyx.mq.consumer;

import com.zyx.es.EsForward;
import com.zyx.main.RelogConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.zyx.main.Constants.PROD_MODE;
import static com.zyx.main.Constants.ZK_CONNECTOR;

/**
 * Created by dolphineor on 2015-5-25.
 */
public class HLConsumerGroup {

    private static final ResourceBundle BUNDLE = ResourceBundle.getBundle("consumer");

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;


    public HLConsumerGroup(String a_topic, int threadNumber, List<EsForward> forwards) {
        this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
        this.topic = a_topic;
        run(threadNumber, forwards);
    }

    public void shutdown() {
        if (consumer != null)
            consumer.shutdown();

        if (executor != null)
            executor.shutdown();
    }

    private void run(int a_threadNumber, List<EsForward> forwards) {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, a_threadNumber);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        executor = Executors.newFixedThreadPool(a_threadNumber);

//        int threadNumber = 1;
        for (final KafkaStream stream : streams) {
            executor.submit(new LogConsumer(stream, forwards));
//            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        BUNDLE.keySet().forEach(key -> props.put(key, BUNDLE.getString(key)));

        String[] zookeeperConnectors = BUNDLE.getString(ZK_CONNECTOR).split(";");

        if (RelogConfig.getMode().equals(PROD_MODE))
            props.put(ZK_CONNECTOR, zookeeperConnectors[1]);
        else
            props.put(ZK_CONNECTOR, zookeeperConnectors[0]);

        return new ConsumerConfig(props);
    }

}
