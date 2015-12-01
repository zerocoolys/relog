package com.zyx.quartz;

//import org.quartz.*;
//import org.quartz.impl.StdSchedulerFactory;

/**
 * Created by dolphineor on 2015-3-26.
 *
 * @deprecated {@link com.zyx.quartz.TimerManager}
 */
public class QuartzManager {
//
//    private static final SchedulerFactory sf = new StdSchedulerFactory();
//    private static final String JOB_NAME = "ELASTIC_INDEX_JOB";
//    private static final String JOB_GROUP_NAME = "ELASTIC_INDEX_JOB_GROUP";
//    private static final String CRON_EXPRESSION = "0 0 23 * * ?";   // 每天晚上11点触发
//
//
//    protected static void addJob(String jobName, String jobGroup, Job job, String time) {
//        try {
//            Scheduler scheduler = sf.getScheduler();
//            JobDetail jobDetail = JobBuilder
//                    .newJob()
//                    .ofType(job.getClass())
//                    .withIdentity(jobName, jobGroup)
//                    .build();
//
//            CronTrigger trigger = TriggerBuilder
//                    .newTrigger()
//                    .forJob(jobName, jobGroup)
//                    .startNow()
//                    .withSchedule(CronScheduleBuilder.cronSchedule(time))
//                    .build();
//
//            scheduler.scheduleJob(jobDetail, trigger);
//            if (!scheduler.isShutdown())
//                scheduler.start();
//        } catch (SchedulerException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void startJob() {
//        addJob(JOB_NAME, JOB_GROUP_NAME, new IndexJob(), CRON_EXPRESSION);
//    }
}
