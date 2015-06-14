package com.ss.main;

/**
 * Created by dolphineor on 2015-5-26.
 */
public class RelogConfig {

    private static String mode;
    private static String kafkaTopic;
    private static String kwInfoReqUrl;

    public static String getMode() {
        return mode;
    }

    public static void setMode(String mode) {
        RelogConfig.mode = mode;
    }

    public static String getKafkaTopic() {
        return kafkaTopic;
    }

    public static void setKafkaTopic(String kafkaTopic) {
        RelogConfig.kafkaTopic = kafkaTopic;
    }

    public static String getKwInfoReqUrl() {
        return kwInfoReqUrl;
    }

    public static void setKwInfoReqUrl(String host) {
        RelogConfig.kwInfoReqUrl = "http://" + host + "/user/%s/keyword/%s";
    }
}
