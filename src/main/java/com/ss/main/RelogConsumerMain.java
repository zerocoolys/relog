package com.ss.main;

import com.ss.es.EsForward;
import com.ss.es.EsPools;
import com.ss.mq.consumer.HLConsumerGroup;
import org.elasticsearch.client.transport.TransportClient;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dolphineor on 2015-5-27.
 */
public class RelogConsumerMain {

    public static void main(String[] args) {
        RelogConfig.setMode(args[0]);
        RelogConfig.setKafkaTopic(args[1]);
        EsPools.setBulkRequestNumber(Integer.parseInt(args[2]));
        int consumerThreadNumber = Integer.valueOf(args[3]);
        RelogConfig.setKwInfoReqUrl(args[4]);

        List<TransportClient> esClients = new ArrayList<>();
        List<EsForward> esForwards = new ArrayList<>();
        EsPools.getEsClient().forEach(client -> {
            esClients.add(client);
            esForwards.add(new EsForward(client));
        });

        new HLConsumerGroup(RelogConfig.getKafkaTopic(), consumerThreadNumber, esForwards);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            esClients.forEach(TransportClient::close);
        }));

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
