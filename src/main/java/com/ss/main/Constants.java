package com.ss.main;

/**
 * Created by baizz on 2015-3-18.
 */
public interface Constants {

    String DEV_MODE = "dev";
    String PROD_MODE = "prod";

    String ZK_CONNECTOR = "zookeeper.connect";
    String KAFKA_BROKER = "metadata.broker.list";

    String REAL_IP = "X-Forwarded-For";

    // Redis key
    String ACCESS_MESSAGE = "access_message";
    String IP_AREA_INFO = "ip_area_information";

    String DELIMITER = "-";

    // Elasticsearch index prefix
    String ACCESS_PREFIX = "access-";
    String VISITOR_PREFIX = "visitor-";

    String ID = "_id";
    String INDEX = "index";
    String TYPE = "_type";

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
    String KW = "kw";       // 搜索词
    String RF_TYPE = "rf_type";     // 1. 直接访问, 2. 搜索引擎, 3. 外部链接
    String DOMAIN = "dm";
    String PATHS = "paths";
    String ENTRANCE = "entrance";   // 入口页面
    String DESTINATION_URL = "des_url";     // 关键词推广URL
    String NEW_VISIT = "n";

    String ET = "et";       // 事件跟踪
    String ET_CATEGORY = "category";    // 监控目标的类型名称
    String ET_ACTION = "action";    // 与目标的交互行为
    String ET_LABEL = "label";      // 事件的额外信息
    String ET_VALUE = "value";      // 事件的额外数值信息

    String KEYWORD_INFO_REQUEST_URL = "http://182.92.227.79:9080/user/%s/keyword/%s";
    String SEM_ACCOUNT_ID = "accountId";
    String SEM_CAMPAIGN_ID = "campaignId";
    String SEM_CAMPAIGN_NAME = "campaignName";
    String SEM_ADGROUP_ID = "adgroupId";
    String SEM_ADGROUP_NAME = "adgroupName";
    String SEM_KEYWORD_NAME = "keywordName";
    String SEM_KEYWORD_IDENTIFIER = "ca";

    String ES_ACCOUNT_ID = "acid";
    String ES_CAMPAIGN_ID = "cid";
    String ES_CAMPAIGN_NAME = "cpna";
    String ES_ADGROUP_ID = "agid";
    String ES_ADGROUP_NAME = "agna";
    String ES_KEYWORD_ID = "kwid";
    String ES_KEYWORD_NAME = "kwna";

}
