package com.zyx.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.alibaba.fastjson.JSON;

import clojure.main;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;





public class PandantTools {
	
	
	public static List<Map<String, Object>> parseJSON2List(String jsonStr){
        JSONArray jsonArr = JSONArray.fromObject(jsonStr);
        List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
        Iterator<JSONObject> it = jsonArr.iterator();
        while(it.hasNext()){
            JSONObject json2 = it.next();
            list.add(parseJSON2Map(json2.toString()));
        }
        return list;
    }
	
	
	 public static Map<String, Object> parseJSON2Map(String jsonStr){
	        Map<String, Object> map = new HashMap<String, Object>();
	        //最外层解析
	        JSONObject json = JSONObject.fromObject(jsonStr);
	        for(Object k : json.keySet()){
	            Object v = json.get(k); 
	            //如果内层还是数组的话，继续解析
	            if(v instanceof JSONArray){
	                List<Map<String, Object>> list = new ArrayList<Map<String,Object>>();
	                Iterator<JSONObject> it = ((JSONArray)v).iterator();
	                while(it.hasNext()){
	                    JSONObject json2 = it.next();
	                    list.add(parseJSON2Map(json2.toString()));
	                }
	                map.put(k.toString(), list);
	            } else {
	                map.put(k.toString(), v);
	            }
	        }
	        return map;
	    }
	 
	 
	 public static String nextCode() {
		 UUID uuid = UUID.randomUUID();
		 return uuid.toString().toUpperCase().replaceAll("-", "");
		 }
	 public static void main(String[] args) {
			String str = "{tt=\"C6DAFB3124D00001CC201180DCD01689\", loc=\"http://www.3renwx.com/member/course.html\", _ucv=\"7d06acead8c84b43b28e747ebd18be57\", city=\"北京市\", fl=\"19.0\", isp=\"-\", User-Agent=\"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36\", Accept-Encoding=\"gzip,deflate,sdch\", remote=\"60.247.41.24\", br=\"Chrome31\", sc=\"32-bit\", dt=\"1448595526927\", vid=\"C6DACEDAE1300001C5B11EC015297820\", tit=\"三仁考研 - Course\", sr=\"1366x768\", Cookie=\"_ucv=7d06acead8c84b43b28e747ebd18be57\", method=\"GET\", Accept=\"image/webp,*/*;q=0.8\", os=\"Windows 7\", utime=\"1448595522222\", X-Forwarded-Host=\"log.best-ad.cn\", Connection=\"close\", ck=\"1\", index=\"access-2015-11-27\", Host=\"log.best-ad.cn\", version=\"HTTP/1.0\", tc=\"0\", ct=\"1\", t=\"3db3beeee5c902017084bee4c1da9abc\", rf=\"http://www.3renwx.com/mini.html?forward=http%3A%2F%2Fwww.3renwx.com%2F\", v=\"1.0.21\", ja=\"1\", Accept-Language=\"zh-CN,zh;q=0.8\", lg=\"zh-CN\", region=\"北京市\", pm=\"0\"}";
			String result = "{tt=C6DAFB3124D00001CC201180DCD01689, loc=http://www.3renwx.com/member/course.html, _ucv=7d06acead8c84b43b28e747ebd18be57, city=北京市, fl=19.0, isp=-, User-Agent=Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36, Accept-Encoding=gzip,deflate,sdch, remote=60.247.41.24, br=Chrome31, sc=32-bit, dt=1448595526927, vid=C6DACEDAE1300001C5B11EC015297820, tit=三仁考研 - Course, sr=1366x768, Cookie=_ucv=7d06acead8c84b43b28e747ebd18be57, method=GET, Accept=image/webp,*/*;q=0.8, os=Windows 7, utime=1448595522222, X-Forwarded-Host=log.best-ad.cn, Connection=close, ck=1, index=access-2015-11-27, Host=log.best-ad.cn, version=HTTP/1.0, tc=0, ct=1, t=3db3beeee5c902017084bee4c1da9abc, rf=http://www.3renwx.com/mini.html?forward=http%3A%2F%2Fwww.3renwx.com%2F, v=1.0.21, ja=1, Accept-Language=zh-CN,zh;q=0.8, lg=zh-CN, region=北京市, pm=0}";

			
//			Map<String, Object> mapSource = (Map<String, Object>) JSON.parse(new String(result));
			
			Map<String, Object> map = parseJSON2Map(str);
//			System.out.println(map);
			
			System.out.println(nextCode());
	
	 }

}
