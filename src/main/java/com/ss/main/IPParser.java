package com.ss.main;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by baizz on 2015-3-18.
 */
public class IPParser {

    private static final String IP_URL_TEMPLATE = "http://ip.taobao.com/service/getIpInfo.php?ip=%s";
    private static final String IP_CHECK_ADDRESS = "http://city.ip138.com/ip2city.asp";
    private static final String IP_REGION_SUFFIX1 = "自治区";
    private static final String IP_REGION_SUFFIX2 = "内蒙古自治区";
    private static final String IP_REGION_SUFFIX3 = "特别行政区";


    public static Map<String, String> getIpInfo(String ip) throws IOException {
        HttpURLConnection conn = null;
        try {
            URL url = new URL(String.format(IP_URL_TEMPLATE, ip));
            conn = (HttpURLConnection) url.openConnection();
            conn.connect();
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));

            String jsonStr = reader.lines().findFirst().get();
            JSONObject jsonObject = JSON.parseObject(jsonStr).getJSONObject("data");

            Map<String, String> ipInfoMap = new HashMap<>();
            String region = jsonObject.getString("region");
            if (region.isEmpty()) {
                ipInfoMap.put("region", "国外");
                ipInfoMap.put("city", "-");
                ipInfoMap.put("isp", "-");
            } else {
                if (IP_REGION_SUFFIX2.equals(region))
                    ipInfoMap.put("region", region.substring(0, 3));
                else {
                    if (region.contains(IP_REGION_SUFFIX1) || region.contains(IP_REGION_SUFFIX3))
                        ipInfoMap.put("region", region.substring(0, 2));
                    else
                        ipInfoMap.put("region", region.replace("省", ""));
                }

                if (jsonObject.getString("city").isEmpty())
                    ipInfoMap.put("city", "-");
                else
                    ipInfoMap.put("city", jsonObject.getString("city"));


                if (jsonObject.getString("isp").isEmpty())
                    ipInfoMap.put("isp", "-");
                else
                    ipInfoMap.put("isp", jsonObject.getString("isp"));

            }

            return ipInfoMap;
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
