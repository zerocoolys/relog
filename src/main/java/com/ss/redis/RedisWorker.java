package com.ss.redis;

import com.alibaba.fastjson.JSON;
import com.ss.es.EsForward;
import com.ss.main.Constants;
import com.ss.parser.IPParser;
import org.elasticsearch.common.collect.Maps;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Created by dolphineor on 2015-3-26.
 *
 * @deprecated Use {@link com.ss.mq.consumer.HLConsumerGroup} instead.
 */
@SuppressWarnings("unchecked")
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
                        String remoteIp = mapSource.get(REMOTE).toString().split(":")[0];
                        String ipInfo = jedis.hget(IP_AREA_INFO, remoteIp);
                        Map<String, Object> ipMap = null;
                        if (ipInfo == null) {
                            ipMap = IPParser.getIpInfo(remoteIp);
                            jedis.hset(IP_AREA_INFO, remoteIp, JSON.toJSONString(ipMap));
                        } else
                            ipMap = (Map<String, Object>) JSON.parse(ipInfo);

                        if (ipMap != null) {
                            ipMap.forEach(mapSource::put);

                            for (EsForward esForward : forwards)
                                esForward.add(Maps.newHashMap(mapSource));
                        }
                    }
                } catch (NullPointerException e) {
                    e.printStackTrace();
                } finally {
                    if (jedis != null)
                        jedis.close();
                }
            }
        });

    }
}
