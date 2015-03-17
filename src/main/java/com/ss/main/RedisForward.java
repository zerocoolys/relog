package com.ss.main;

import com.ss.config.JRedisPools;
import com.ss.vo.MessageObject;
import redis.clients.jedis.Jedis;

/**
 * Created by baizz on 2015-3-17.
 */
public class RedisForward {

    public static final String ACCESS_MESSAGE = "access_message";


    public static void push(MessageObject httpMessage) {
        Jedis jedis = null;
        try {
            jedis = JRedisPools.getConnection();
            jedis.set("", "");
        } finally {
            JRedisPools.returnJedis(jedis);
        }
    }
}
