package com.ss.main;

/**
 * Created by dolphineor on 2015-5-26.
 */
public class RelogConfig {

    private static String mode;
    private static String kafkaTopic;

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
}
