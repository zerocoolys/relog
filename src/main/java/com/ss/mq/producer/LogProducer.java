package com.ss.mq.producer;

import com.ss.main.Constants;
import com.ss.main.RelogConfig;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Objects;
import java.util.Properties;
import java.util.Random;
import java.util.ResourceBundle;

/**
 * Created by dolphineor on 2015-5-25.
 */
public class LogProducer implements Constants {
    private static final ResourceBundle BUNDLE = ResourceBundle.getBundle("kafka");
    private static final Random RANDOM = new Random();
    private static final String EMPTY_DELIMITER = "";
    private static final int RANDOM_RANGE = 100;

    private final Producer<String, String> producer;
    private final String topic;


    public LogProducer(String topic) {
        this.producer = new Producer<>(createProducerConfig());
        this.topic = topic;
    }

    public LogProducer(ProducerConfig config, String topic) {
        this.producer = new Producer<>(config);
        this.topic = topic;
    }

    public void handleMessage(String msg) {
        KeyedMessage<String, String> data = new KeyedMessage<>(topic, RANDOM.nextInt(RANDOM_RANGE) + EMPTY_DELIMITER, msg);
        producer.send(data);
    }

    public void close() {
        Objects.requireNonNull(producer);
        producer.close();
    }

    private static ProducerConfig createProducerConfig() {
        Properties props = new Properties();
        BUNDLE.keySet().forEach(key -> props.put(key, BUNDLE.getString(key)));
        String[] kafkaBrokers = BUNDLE.getString(KAFKA_BROKER).split(";");
        if (RelogConfig.getMode().equals(PROD_MODE))
            props.put(KAFKA_BROKER, kafkaBrokers[1]);
        else
            props.put(KAFKA_BROKER, kafkaBrokers[0]);

        return new ProducerConfig(props);
    }

}