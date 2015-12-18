package com.zyx.kafka;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
/**
 * 
 * @description 模拟慧眼像kafka发送数据
 * @author ZhangHuaRong   
 * @update 2015年12月3日 下午3:28:49
 */

public class ProducerDemo {
    public static void main(String[] args) {
        Random rnd = new Random();
        int events=50;
 
        // 设置配置属性
        Properties props = new Properties();
        props.put("metadata.broker.list","192.168.1.110:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        // key.serializer.class默认为serializer.class
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 可选配置，如果不配置，则使用默认的partitioner
        props.put("partitioner.class", "com.kafkasend.PartitionerDemo");
        props.put("zk.connectiontimeout.ms", "15000");
        // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
        // 值为0,1,-1,可以参考
        // http://kafka.apache.org/08/configuration.html
        props.put("request.required.acks", "1");
        ProducerConfig config = new ProducerConfig(props);
 
        // 创建producer
        Producer<String, String> producer = new Producer<String, String>(config);
        // 产生并发送消息
        
        String msg = getTestMsg();
        
        long start=System.currentTimeMillis();
        for (long i = 0; i < events; i++) {
            String runtime = new Date().toLocaleString();
            String ip = "192.168.5." + i;//rnd.nextInt(255);
           
            //如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
            KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                    "domain10", ip, msg);
            	  producer.send(data);
            	  if(i%3==0){
            		  System.out.println("send data:"+i+data);
            	  }
            try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
        System.out.println("耗时:" + (System.currentTimeMillis() - start));
        // 关闭producer
        producer.close();
    }

	private static String getTestMsg() {
		String result = "{tt=C6DAFB3124D00001CC201180DCD01689, loc=http://www.3renwx.com/member/course.html, _ucv=7d06acead8c84b43b28e747ebd18be57, city=北京市, fl=19.0, isp=-, User-Agent=Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36, Accept-Encoding=gzip,deflate,sdch, remote=60.247.41.24, br=Chrome31, sc=32-bit, dt=1448595526927, vid=C6DACEDAE1300001C5B11EC015297820, tit=三仁考研 - Course, sr=1366x768, Cookie=_ucv=7d06acead8c84b43b28e747ebd18be57, method=GET, Accept=image/webp,*/*;q=0.8, os=Windows 7, utime=1448595522222, X-Forwarded-Host=log.best-ad.cn, Connection=close, ck=1, index=access-2015-11-27, Host=log.best-ad.cn, version=HTTP/1.0, tc=0, ct=1, t=3db3beeee5c902017084bee4c1da9abc, rf=http://www.3renwx.com/mini.html?forward=http%3A%2F%2Fwww.3renwx.com%2F, v=1.0.21, ja=1, Accept-Language=zh-CN,zh;q=0.8, lg=zh-CN, region=北京市, pm=0}";
		String str = "{tt=\"C6DAFB3124D00001CC201180DCD01689\", loc=\"http://www.3renwx.com/member/course.html\", _ucv=\"7d06acead8c84b43b28e747ebd18be57\", city=\"北京市\", fl=\"19.0\", isp=\"-\", User-Agent=\"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36\", Accept-Encoding=\"gzip,deflate,sdch\", remote=\"60.247.41.24\", br=\"Chrome31\", sc=\"32-bit\", dt=\"1448595526927\", vid=\"C6DACEDAE1300001C5B11EC015297820\", tit=\"三仁考研 - Course\", sr=\"1366x768\", Cookie=\"7d06acead8c84b43b28e747ebd18be57\", method=\"GET\", Accept=\"image/webp,*/*;q=0.8\", os=\"Windows 7\", utime=\"1448595522222\", X-Forwarded-Host=\"log.best-ad.cn\", Connection=\"close\", ck=\"1\", index=\"access-2015-12-05\", Host=\"log.best-ad.cn\", version=\"HTTP/1.0\", tc=\"0\", ct=\"1\", t=\"3db3beeee5c902017084bee4c1da9abc\", rf=\"http://www.3renwx.com/mini.html?forward=http%3A%2F%2Fwww.3renwx.com%2F\", v=\"1.0.21\", ja=\"1\", Accept-Language=\"zh-CN,zh;q=0.8\", lg=\"zh-CN\", region=\"北京市\", pm=\"0\"}";

		return str;
	}
}
