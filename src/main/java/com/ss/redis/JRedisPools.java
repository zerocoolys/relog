package com.ss.redis;

import com.ss.main.RelogConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import java.util.ResourceBundle;

import static com.ss.main.Constants.DEV_MODE;

/**
 * Created by dolphineor on 2015-03-17.
 */
public class JRedisPools {

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
        String portStr = bundle.getString("redis.port");
        String password = bundle.getString("redis.password");
        if (DEV_MODE.equals(RelogConfig.getMode())) {
            hostName = hostName.split(";")[0];
            portStr = portStr.split(";")[0];
            password = password.split(";")[0];
        } else {
            hostName = hostName.split(";")[1];
            portStr = portStr.split(";")[1];
            password = password.split(";")[1];
        }

        jedisPool = new JedisPool(config, hostName, Integer.valueOf(portStr), Protocol.DEFAULT_TIMEOUT, password);
    }


    public static Jedis getConnection() {
        return jedisPool.getResource();
    }

}
