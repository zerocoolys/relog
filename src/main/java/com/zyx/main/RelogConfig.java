package com.zyx.main;

/**
 * Created by dolphineor on 2015-5-26.
 */
public class RelogConfig {

    private static String mode;
    private static String topic;
    private static String groupId;

    public static String getMode() {
        return mode;
    }

    public static void setMode(String mode) {
        RelogConfig.mode = mode;
    }

    public static String getTopic() {
        return topic;
    }

    public static void setTopic(String topic) {
        RelogConfig.topic = topic;
    }

    public static String getGroupId() {
        return groupId;
    }

    public static void setGroupId(String groupId) {
        RelogConfig.groupId = groupId;
    }

}
