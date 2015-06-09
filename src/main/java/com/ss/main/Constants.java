package com.ss.main;

/**
 * Created by dolphineor on 2015-3-18.
 */
public interface Constants {

    String DEV_MODE = "dev";
    String PROD_MODE = "prod";

    String ZK_CONNECTOR = "zookeeper.connect";
    String KAFKA_BROKER = "metadata.broker.list";

    String EMPTY_STRING = "";

    String HTTP_PATH = "path";
    String HTTP_PREFIX = "http:";
    String WWW_PREFIX = "www.";
    String DOUBLE_SLASH = "://";
    String REAL_IP = "X-Forwarded-For";
    String REFERRER = "Referer";
    String DT = "dt";

    // Redis key
    String ACCESS_MESSAGE = "access_message";
    String IP_AREA_INFO = "ip_area_information";

    String PLACEHOLDER = "-";
    String QUESTION_MARK = "?";

    // Elasticsearch index prefix
    String ACCESS_PREFIX = "access-";
    @Deprecated
    String VISITOR_PREFIX = "visitor-";
    String ES_TYPE_EVENT_SUFFIX = "_event";
    String ES_TYPE_XY_SUFFIX = "_xy";
    String ES_TYPE_PROMOTION_URL_SUFFIX = "_promotion_url";

    String ID = "_id";      // elasticsearch文档id
    String INDEX = "index"; // elasticsearch索引名称
    String TYPE = "_type";  // elasticsearch文档类型

    // Elasticsearch field
    String METHOD = "method";
    String VERSION = "version";
    String REMOTE = "remote";       // IP
    String REGION = "region";       // 地域信息(省份, 直辖市等)
    String CITY = "city";           // 城市
    String ISP = "isp";             // 网络提供商
    String CURR_ADDRESS = "loc";    // 当前访问的页面
    String UNIX_TIME = "utime";     // 访问当前页面的时间
    String T = "t";         // trackId
    String TT = "tt";       // 访问次数标识符
    String VID = "vid";     // 访客唯一标识符
    String UCV = "_ucv";    // 访客数(UV)区分标识符
    String RF = "rf";       // Referrer
    String SE = "se";       // 搜索引擎名称
    String KW = "kw";       // 搜索词
    String RF_TYPE = "rf_type";     // 1. 直接访问, 2. 搜索引擎, 3. 外部链接
    String DOMAIN = "dm";           // 依据rf解析出的domain
    String PATHS = "paths";         // 访问当前页面的路径信息
    String ENTRANCE = "entrance";   // 入口页面(1->入口页面, 0->普通页面)
    String DESTINATION_URL = "des_url";     // 关键词推广URL
    String NEW_VISIT = "n";         // 是否一次新的访问
    String XY = "xy";               // xy坐标信息

    // Elasticsearch 推广URL字段信息
    String UT = "ut";
    String DMS = "dms";
    String LET = "let";
    String NETT = "nett";
    String NETD = "netd";
    String NTTP = "nttp";
    String SRV = "srv";

    String ET = "et";               // 事件跟踪
    String ET_CATEGORY = "et_category";    // 监控目标的类型名称
    String ET_ACTION = "et_action";    // 与目标的交互行为
    String ET_LABEL = "et_label";      // 事件的额外信息
    String ET_VALUE = "et_value";      // 事件的额外数值信息

    String KEYWORD_INFO_REQUEST_URL = "http://182.92.227.79:9080/user/%s/keyword/%s";
    String SEM_ACCOUNT_ID = "accountId";
    String SEM_CAMPAIGN_ID = "campaignId";
    String SEM_CAMPAIGN_NAME = "campaignName";
    String SEM_ADGROUP_ID = "adgroupId";
    String SEM_ADGROUP_NAME = "adgroupName";
    String SEM_KEYWORD_NAME = "keywordName";
    String SEM_KEYWORD_IDENTIFIER = "ca";

    // Elasticsearch 推广信息的字段类型
    String ES_ACCOUNT_ID = "acid";      // 帐户id
    String ES_CAMPAIGN_ID = "cid";      // 推广计划id
    String ES_CAMPAIGN_NAME = "cpna";   // 推广计划名称
    String ES_ADGROUP_ID = "agid";      // 推广单元id
    String ES_ADGROUP_NAME = "agna";    // 推广单元名称
    String ES_KEYWORD_ID = "kwid";      // 推广关键词id
    String ES_KEYWORD_NAME = "kwna";    // 推广关键词名称


    String VAL_RF_TYPE_DIRECT = "1";
    String VAL_RF_TYPE_SE = "2";
    String VAL_RF_TYPE_OUTLINK = "3";
    String VAL_RF_TYPE_SITES = "0";

}
