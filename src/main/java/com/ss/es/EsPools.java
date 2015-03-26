package com.ss.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by baizz on 2015-3-20.
 */
public class EsPools {

    private static String host = null;
    private static String clusterName = null;
    private static int bulkRequestNumber;

    private static List<TransportClient> clients = new ArrayList<>();
    private static Map<String, String> esMap = new HashMap<>();


    private EsPools() {
    }

    public static List<TransportClient> getEsClient() {
        if (clients.isEmpty()) {
            synchronized (EsPools.class) {
                if (clients.isEmpty()) {
                    String[] hosts = host.split(",");
                    String[] clusterNames = clusterName.split(",");
                    for (int i = 0, l = hosts.length; i < l; i++)
                        clients.add(initEsClient(hosts[i], clusterNames[i]));
                }
            }
        }

        return clients;
    }

    private static TransportClient initEsClient(String host, String clusterName) {
        TransportClient client = null;
        try {
            String[] arr = host.split(":");

            // 设置client.transport.sniff为true来使客户端去嗅探整个集群的状态, 把集群中其它机器的ip地址加到客户端中
            Settings settings = ImmutableSettings.settingsBuilder().put(esMap)
                    .put("cluster.name", clusterName)
                    .put("client.transport.sniff", true)
                    .put("client.transport.ignore_cluster_name", true)
                    .put("client.transport.ping_timeout", "10s")
                    .put("client.transport.nodes_sampler_interval", "15s").build();
            client = new TransportClient(settings);

            if (arr.length == 1)
                client.addTransportAddress(new InetSocketTransportAddress(arr[0], 19300));
            else if (arr.length == 2)
                client.addTransportAddress(new InetSocketTransportAddress(arr[0], Integer.parseInt(arr[1])));
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return client;
    }


    public static void setHost(String host) {
        EsPools.host = host;
    }

    public static void setClusterName(String clusterName) {
        EsPools.clusterName = clusterName;
    }

    public static int getBulkRequestNumber() {
        return bulkRequestNumber;
    }

    public static void setBulkRequestNumber(int bulkRequestNumber) {
        EsPools.bulkRequestNumber = bulkRequestNumber;
    }
}
