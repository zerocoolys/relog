package com.ss.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by baizz on 2015-3-20.
 */
public class EsPools {

    private static String host = null;
    private static int port;
    private static String clusterName = null;
    private static int bulkRequestNumber;

    private static TransportClient esClient = null;
    private static Map<String, String> esMap = new HashMap<>();


    private EsPools() {
    }

    //retrieve an es client instance
    public static TransportClient getEsClient() {
        if (esClient == null) {
            synchronized (EsPools.class) {
                if (esClient == null)
                    esClient = initEsClient();
            }
        }

        return esClient;
    }

    private static TransportClient initEsClient() {
        TransportClient client = null;
        try {
            // 设置client.transport.sniff为true来使客户端去嗅探整个集群的状态, 把集群中其它机器的ip地址加到客户端中
            Settings settings = ImmutableSettings.settingsBuilder().put(esMap).put("cluster.name", getClusterName()).put("client.transport.sniff", true).build();
            Class<?> clazz = Class.forName(TransportClient.class.getName());
            Constructor<?> constructor = clazz.getDeclaredConstructor(Settings.class);
            constructor.setAccessible(true);
            client = (TransportClient) constructor.newInstance(settings);
            client.addTransportAddresses(new InetSocketTransportAddress(getHost(), getPort()));
        } catch (final Exception e) {
            e.printStackTrace();
        }

        return client;
    }

    public static String getHost() {
        return host;
    }

    public static void setHost(String host) {
        EsPools.host = host;
    }

    public static int getPort() {
        return port;
    }

    public static void setPort(int port) {
        EsPools.port = port;
    }

    public static String getClusterName() {
        return clusterName;
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
