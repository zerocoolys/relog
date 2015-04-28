package com.ss.quartz;

import java.util.Calendar;
import java.util.Timer;

/**
 * Created by baizz on 2015-4-28.
 */
public class TimerManager {

    private static final int PERIOD_DAY = 86_400_000;

    private static final int PERIOD_HOUR = 3_600_000;


    public static void startJob() {
        Timer timer = new Timer();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, -PERIOD_HOUR);
        timer.scheduleAtFixedRate(new IndexTask(), calendar.getTime(), PERIOD_DAY);
    }

}
