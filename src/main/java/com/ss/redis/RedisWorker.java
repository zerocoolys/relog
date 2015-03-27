package com.ss.redis;

import com.alibaba.fastjson.JSON;
import com.ss.es.EsForward;
import com.ss.main.Constants;
import com.ss.main.IPParser;
import redis.clients.jedis.Jedis;

import java.io.IOException;
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
                        String remoteIp = mapSource.get("remote").toString().split(":")[0];
                        String ipInfo = jedis.hget(IP_AREA_INFO, remoteIp);
                        Map<String, String> ipMap = null;
                        if (ipInfo == null) {
                            ipMap = IPParser.getIpInfo(remoteIp);
                            jedis.hset(IP_AREA_INFO, remoteIp, JSON.toJSONString(ipMap));
                        } else
                            ipMap = (Map<String, String>) JSON.parse(ipInfo);

                        ipMap.forEach(mapSource::put);

                        for (EsForward esForward : forwards)
                            esForward.add(mapSource);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    JRedisPools.returnJedis(jedis);
                }
            }
        });

    }
}
