package com.ss.redis;

import com.ss.main.Constants;
import com.ss.main.RelogConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.ResourceBundle;

/**
 * Created by baizz on 2015-03-17.
 */
public class JRedisPools implements Constants {

    private static JedisPool jedisPool;


    static {
        if (jedisPool == null) {
            synchronized (JRedisPools.class) {
                if (jedisPool == null)
                    init();
            }
        }
    }

    private static void init() {
        ResourceBundle bundle = ResourceBundle.getBundle("redis");
        if (bundle == null) {
            throw new IllegalArgumentException(
                    "[redis.properties] is not found!");
        }
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(Integer.valueOf(bundle
                .getString("redis.pool.maxActive")));
        config.setMaxIdle(Integer.valueOf(bundle
                .getString("redis.pool.maxIdle")));
        config.setMaxWaitMillis(Integer.valueOf(bundle
                .getString("redis.pool.maxWait")));
        config.setTestOnBorrow(Boolean.valueOf(bundle
                .getString("redis.pool.testOnBorrow")));
        config.setTestOnReturn(Boolean.valueOf(bundle
                .getString("redis.pool.testOnReturn")));

        String hostName = bundle.getString("redis.hostName");
        if (DEV_MODE.equals(RelogConfig.getMode()))
            hostName = hostName.split(",")[0];
        else
            hostName = hostName.split(",")[1];

        jedisPool = new JedisPool(config, hostName,
                Integer.valueOf(bundle.getString("redis.port")),
                Protocol.DEFAULT_TIMEOUT,
                bundle.getString("redis.password"));
    }


    public static Jedis getConnection() {
        return jedisPool.getResource();
    }

}
