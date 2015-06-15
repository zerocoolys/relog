package com.ss.mq.consumer;

import com.alibaba.fastjson.JSON;
import com.ss.es.EsForward;
import com.ss.main.Constants;
import com.ss.monitor.MonitorService;
import com.ss.parser.IPParser;
import com.ss.redis.JRedisPools;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.elasticsearch.common.collect.Maps;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by dolphineor on 2015-5-25.
 */
public class LogConsumer implements Runnable, Constants {

    private List<EsForward> forwards;
    private KafkaStream m_stream;
//    private int m_threadNumber;

    public LogConsumer(KafkaStream m_stream, List<EsForward> forwards) {
        this.m_stream = m_stream;
//        this.m_threadNumber = m_threadNumber;
        this.forwards = forwards;
    }


    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        MonitorService.getService().mq_receive();
        for (ConsumerIterator<byte[], byte[]> it = m_stream.iterator(); it.hasNext(); ) {
            Jedis jedis = null;
            try {
                jedis = JRedisPools.getConnection();
                byte[] msg = it.next().message();
                Map<String, Object> mapSource = (Map<String, Object>) JSON.parse(new String(msg));
                String remoteIp = mapSource.get(REMOTE).toString().split(":")[0];
                String ipInfo = jedis.hget(IP_AREA_INFO, remoteIp);
                Map<String, String> ipMap;
                if (ipInfo == null) {
                    ipMap = IPParser.getIpInfo(remoteIp);
                    jedis.hset(IP_AREA_INFO, remoteIp, JSON.toJSONString(ipMap));
                } else
                    ipMap = (Map<String, String>) JSON.parse(ipInfo);

                if (ipMap != null) {
                    ipMap.forEach(mapSource::put);

                    for (EsForward esForward : forwards) {
                        esForward.add(Maps.newHashMap(mapSource));
                        MonitorService.getService().es_forwarded();
                    }
                }
            } catch (IOException | NullPointerException e) {
                e.printStackTrace();
            } finally {
                if (jedis != null)
                    jedis.close();
            }
        }
    }

}
