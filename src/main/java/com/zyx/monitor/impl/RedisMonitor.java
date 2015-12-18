package com.zyx.monitor.impl;

import com.zyx.monitor.Monitor;
import com.zyx.redis.JRedisPools;
import redis.clients.jedis.Jedis;

/**
 * Created by yousheng on 15/6/15.
 */
public class RedisMonitor implements Monitor {

    private static final String PREFIX = "RELOG:";

    private static final String HTTP_RECEIVED = "HTTP_RECEIVED-1"; //1
    private static final String MQ_KEY_SEND = "MQ_KEY_SEND-2";//2
    private static final String MQ_KEY_RECEIVE = "MQ_KEY_RECEIVE-3";//3
    private static final String ES_FORWARDED = "ES_FORWARDED-4";//4
    private static final String DATA_READY = "DATA_READY-5";//5
    private static final String ES_DATA_READY = "ES_DATA_READY-6";//6
    private static final String ES_DATA_SAVED = "ES_DATA_SAVED-7";//7

    private static final String MQ_KEY_FAILED = "RELOG_KAFKA_MQ_FAILED";
    private static final String ES_KEY_FAILED = "RELOG_ES_FAILED";
    private static final String DATA_ERROR = "DATA_ERROR";
    private static final String ES_DATA_ERROR = "ES_DATA_ERROR";


    private void update(String key, int value) {

        Jedis jedis = null;
        try {

            jedis = JRedisPools.getConnection();

            if (jedis == null)
                return;

            jedis.incrBy(PREFIX + key, value);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }

    @Override
    public void data_ready() {
        update(DATA_READY, 1);
    }

    @Override
    public void success_http() {
        update(HTTP_RECEIVED, 1);
    }

    @Override
    public void mq_send() {
        update(MQ_KEY_SEND, 1);
    }

    @Override
    public void mq_receive() {
        update(MQ_KEY_RECEIVE, 1);
    }

    @Override
    public void failed_mq(int num) {
        update(MQ_KEY_FAILED, num);
    }

    @Override
    public void es_forwarded() {
        update(ES_FORWARDED, 1);
    }

    @Override
    public void failed_es(int num) {
        update(ES_KEY_FAILED, num);
    }

    @Override
    public void data_error() {
        update(DATA_ERROR, 1);
    }

    @Override
    public void es_data_error() {
        update(ES_DATA_ERROR, 1);
    }

    @Override
    public void es_data_saved(int num) {
        update(ES_DATA_SAVED, num);
    }

    @Override
    public void es_data_ready() {
        update(ES_DATA_READY, 1);
    }
}
