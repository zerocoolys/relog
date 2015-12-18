package com.zyx.monitor;

import com.zyx.monitor.impl.RedisMonitor;

/**
 * Created by yousheng on 15/6/15.
 */
public class MonitorService {


    private static Monitor monitor = new RedisMonitor();


    public static Monitor getService() {
        return monitor;
    }

    public MonitorService(Monitor monitor) {
        MonitorService.monitor = monitor;
    }


}
