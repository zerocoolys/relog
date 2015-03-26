package com.ss.redis;

import com.alibaba.fastjson.JSON;
import com.ss.es.EsForward;
import com.ss.main.Constants;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Created by baizz on 2015-3-26.
 */
public class RedisWorker implements Constants {

    public RedisWorker(List<EsForward> forwards) {
        init(forwards);
    }

    private void init(List<EsForward> forwards) {
        Executors.newSingleThreadExecutor().execute(() -> {
            while (true) {
                Jedis jedis = null;
                try {
                    jedis = JRedisPools.getConnection();
                    String source = jedis.rpop(ACCESS_MESSAGE);
                    if (source != null) {
                        Map<String, Object> mapSource = (Map<String, Object>) JSON.parse(source);
                        for (EsForward esForward : forwards) {
                            esForward.add(mapSource);
                        }
                    }
                } finally {
                    JRedisPools.returnJedis(jedis);
                }
            }
        });

    }
}
