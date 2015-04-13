package com.ss.main;

/**
 * Created by baizz on 2015-3-18.
 */
public interface Constants {

    // Redis key
    String ACCESS_MESSAGE = "access_message";
    String IP_AREA_INFO = "ip_area_information";

    // Elasticsearch field
    String REMOTE = "remote";
    String METHOD = "method";
    String VERSION = "version";
    String REGION = "region";
    String CITY = "city";
    String ISP = "isp";
    String CURR_ADDRESS = "loc";
    String UNIX_TIME = "utime";
    String T = "t";         // trackId
    String TT = "tt";       // 访问次数标识符
    String VID = "vid";     // 访客唯一标识符
    String UCV = "_ucv";    // 访客数(UV)区分标识符
    String RF = "rf";
    String SE = "se";
    String KW = "kw";

}
