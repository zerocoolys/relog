package com.zyx.storm.topologies.trident;

import java.util.Map;

import org.elasticsearch.common.Strings;

import com.zyx.main.Constants;
import com.zyx.redis.JRedisPools;
import com.zyx.utils.PandantTools;
import com.zyx.utils.UrlUtils;

import redis.clients.jedis.Jedis;
import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;
/**
 * 
 * @description 数据有效性检查
 * @author ZhangHuaRong   
 * @update 2015年12月3日 下午3:28:49
 */
public class ValidityFilter extends BaseFilter implements Constants {

	@Override
	public boolean isKeep(TridentTuple tuple) {
		boolean flag = true;
	/*	try {
			Map<String, Object> mapSource = PandantTools.parseJSON2Map(tuple.getString(0));
			//check tt t
			if (!mapSource.containsKey(T) || !mapSource.containsKey(TT))
				flag = false;
			
			//check trackid from redis
			  Jedis jedis = null;
			  jedis = JRedisPools.getConnection();
              String trackId = mapSource.getOrDefault(T, EMPTY_STRING).toString();
              String esType = jedis.get(TYPE_ID_PREFIX + trackId);
              if (esType == null)
            	  flag = false;
              
             // 网站代码安装的正确性检测
              String siteUrl = jedis.get(SITE_URL_PREFIX + trackId);
              if (Strings.isEmpty(siteUrl) || !UrlUtils.match(siteUrl, mapSource.get(CURR_ADDRESS).toString()))
            	  flag = false;
			  
			  
		} catch (Exception e) {
			e.printStackTrace();
			flag = false;
		}*/
		return flag;
	}

}
