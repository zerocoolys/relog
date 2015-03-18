package com.ss.main;

import com.alibaba.fastjson.JSON;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;

/**
 * Created by baizz on 2015-3-18.
 */
public class IPParser {

    private static final String IP_URL_TEMPLATE = "http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=js&ip=%s";
    private static final String IP_CHECK_ADDRESS = "http://city.ip138.com/ip2city.asp";
    private static final String PREFIX = "var remote_ip_info = ";
    private static final String CITY = "city";


    public static String getArea(String ip) throws IOException {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(String.format(IP_URL_TEMPLATE, ip));
            conn = (HttpURLConnection) url.openConnection();
            conn.connect();
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
            String responseStr = reader.lines().findFirst().get().replace(PREFIX, "").replace(";", "");

            return JSON.parseObject(responseStr).getString(CITY);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
        return null;
    }

    // 获取当前主机在web上的真实IP(该主机有可能没有配置外网IP, 外网不能对当前主机进行访问)
    public static String getWebIp() {
        try {
            URL url = new URL(IP_CHECK_ADDRESS);
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0");
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
            conn.setRequestProperty("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
            conn.setRequestProperty("Accept-language", "zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3");

            StringBuilder response = new StringBuilder("");
            BufferedReader br = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8));
            br.lines().forEach(s -> response.append(s).append("\r\n"));

            String content = response.toString();
            return content.substring(content.indexOf("[") + 1, content.indexOf("]"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String getRealIp() throws SocketException {
        String localIp = null;// 内网IP, 如果没有配置外网IP则返回它
        String netIp = null;// 外网IP

        Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip;
        boolean isFound = false;// 是否找到外网IP
        while (netInterfaces.hasMoreElements() && !isFound) {
            NetworkInterface ni = netInterfaces.nextElement();
            Enumeration<InetAddress> address = ni.getInetAddresses();
            while (address.hasMoreElements()) {
                ip = address.nextElement();
                if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")) {// 外网IP
                    netIp = ip.getHostAddress();
                    isFound = true;
                    break;
                } else if (ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")) {// 内网IP
                    localIp = ip.getHostAddress();
                }
            }
        }

        if (netIp != null && !"".equals(netIp)) {
            return netIp;
        } else {
            return localIp;
        }
    }
}
