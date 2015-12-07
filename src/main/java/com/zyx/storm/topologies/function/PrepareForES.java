package com.zyx.storm.topologies.function;

import backtype.storm.tuple.Values;
import net.sf.json.JSONArray;
import redis.clients.jedis.Jedis;

import org.json.simple.JSONObject;

import com.google.common.collect.Lists;
import com.zyx.main.Constants;
import com.zyx.parser.SearchEngineParser;
import com.zyx.redis.JRedisPools;
import com.zyx.utils.GaDateUtils;
import com.zyx.utils.PandantTools;
import com.zyx.utils.UrlUtils;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class PrepareForES extends BaseFunction implements Constants {

	private String esIndex, esType;
	
	private static final int ONE_DAY_SECONDS = 86_400;

	public void prepare(Map conf, TridentOperationContext context) {
		/*
		 * this.esIndex = conf.get("ELASTICSEARCH_INDEX_NAME").toString();
		 * this.esType = conf.get("ELASTICSEARCH_TYPE_NAME").toString();
		 */

		this.esIndex = "test-" + GaDateUtils.getCurrentDate();
		this.esType = "test";

	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String esType;
		try {
			String str = tuple.getString(0);
			Map<String, Object> mapSource = PandantTools.parseJSON2Map(str);
			
			Jedis jedis = null;
//			jedis = JRedisPools.getConnection();
//			String trackId = mapSource.getOrDefault(T, EMPTY_STRING).toString();
			String trackId = "222bf2d17cffad63c25ec1528864ca11";
			mapSource.remove(T);

		
//			esType = jedis.get(TYPE_ID_PREFIX + trackId);
			
			esType = "222bf2d17cffad63c25ec1528864ca11";

			long time = Long.parseLong(mapSource.get(UNIX_TIME).toString());
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(time);
			String dateString = DATE_FORMAT.format(calendar.getTime());
			String ipDupliKey = trackId + DELIMITER + dateString;
//			long statusCode = jedis.sadd(ipDupliKey, mapSource.get(REMOTE).toString());
			long statusCode = 0;
			mapSource.put(IP_DUPLICATE, statusCode);
			// 设置过期时间
//			jedis.expire(ipDupliKey, ONE_DAY_SECONDS + 3600);

			// 页面转化
//		pageConversionProcessor.add(mapSource, esType);
			
			
			
			// 区分普通访问, 事件跟踪, xy坐标, 推广URL, 指定广告跟踪统计信息
			String eventInfo = mapSource.getOrDefault(ET, EMPTY_STRING).toString();
			String xyCoordinateInfo = mapSource.getOrDefault(XY, EMPTY_STRING).toString();
			String promotionUrlInfo = mapSource.getOrDefault(UT, EMPTY_STRING).toString();
			String adTrackInfo = mapSource.getOrDefault(AD_TRACK, EMPTY_STRING).toString();
			String pcname = mapSource.getOrDefault(PAGE_CONVERSION_NAME, EMPTY_STRING).toString();//页面转化
//			String siteUrl = jedis.get(SITE_URL_PREFIX + trackId);
			
			String tt = mapSource.get(TT).toString();
			
			
			Map<String, Object> adTrackMap = new HashMap<>();
			if ( !eventInfo.isEmpty()) {
			    mapSource.put(TYPE, esType + ES_TYPE_EVENT_SUFFIX);
			    String locTemp = mapSource.get(CURR_ADDRESS).toString();
			    int lastIndex = locTemp.length() - 1;
			    if (Objects.equals('/', locTemp.charAt(lastIndex))) {
			        locTemp = locTemp.substring(0, lastIndex);
			        mapSource.put(CURR_ADDRESS, locTemp);
			        locTemp = null;
			    }
//            addRequest(client, requestQueue, EventProcessor.handle(mapSource, jedis.get(tt), siteUrl));
//            continue;
			    Map<String, Object> eventmap =  handle(mapSource, jedis.get(tt), "siteUrl");
			    Values events = new Values(mapSource.get("index").toString(),mapSource.get("TYPE").toString(),PandantTools.nextCode(),JSONObject.toJSONString(eventmap));
			    collector.emit(events);
			
			}else{
				 mapSource.put(TYPE, esType);
			}
			
			collector.emit(new Values(mapSource.get("index").toString(),mapSource.get(TYPE).toString(),PandantTools.nextCode(),JSONObject.toJSONString(mapSource)));
			 System.out.println("********************"+JSONObject.toJSONString(mapSource));
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} 

		


	
	}
	
	
	private  Map<String, Object> handle(Map<String, Object> source,	String rf_type, String siteUrl) throws UnsupportedEncodingException {
		if (source.isEmpty())
			return Collections.emptyMap();

		Map<String, Object> sourceMap = new HashMap<>();

		String[] eventArr = source.get(ET).toString().split("\\*");
		sourceMap.put(ET_CATEGORY, eventArr[0]);
		sourceMap.put(ET_ACTION, eventArr[1]);
		if (eventArr.length == 3) {
			sourceMap.put(ET_TARGET, eventArr[2]);
		} else if (eventArr.length == 4) {
			sourceMap.put(ET_TARGET, eventArr[2]);
			sourceMap.put(ET_LABEL, eventArr[3]);
		} else if (eventArr.length == 5) {
			sourceMap.put(ET_TARGET, eventArr[2]);
			sourceMap.put(ET_LABEL, eventArr[3]);
			sourceMap.put(ET_VALUE, eventArr[4]);
		} else {
			sourceMap.put(ET_LABEL, EMPTY_STRING);
			sourceMap.put(ET_VALUE, EMPTY_STRING);
		}

		String tt = source.get(TT).toString();
		String refer = source.get(RF).toString();

		if (PLACEHOLDER.equals(refer) || UrlUtils.match(siteUrl, refer)) { // 直接访问
			sourceMap.put(SE, PLACEHOLDER);

			if (rf_type == null) {
				sourceMap.put(RF_TYPE, rf_type);
			}
		} else {
			List<String> skList = Lists.newArrayList();
			boolean found = SearchEngineParser.getSK(
					java.net.URLDecoder.decode(refer,
							StandardCharsets.UTF_8.name()), skList);
			if (found) { // 搜索引擎
				sourceMap.put(SE, skList.remove(0));
				if (rf_type == null) {
					sourceMap.put(RF_TYPE, VAL_RF_TYPE_SE);
				} else {
					sourceMap.put(RF_TYPE, rf_type);
				}
			} else { // 外部链接
				if (rf_type == null) {
					sourceMap.put(RF_TYPE, VAL_RF_TYPE_OUTLINK);
				} else {
					sourceMap.put(RF_TYPE, rf_type);
				}
			}
		}

		sourceMap.put(INDEX, source.get(INDEX).toString());
		sourceMap.put(TYPE, source.get(TYPE).toString());
		sourceMap.put(TT, source.get(TT).toString());
		sourceMap.put(VID, source.get(VID).toString());
		sourceMap.put(CURR_ADDRESS, source.get(CURR_ADDRESS).toString());
		sourceMap.put(UNIX_TIME,
				Long.parseLong(source.get(UNIX_TIME).toString()));
		sourceMap.put(VISITOR_IDENTIFIER,
				Integer.parseInt(source.get(VISITOR_IDENTIFIER).toString()));
		sourceMap.put(REGION, source.get(REGION).toString());
		sourceMap.put(CITY, source.get(CITY).toString());
		sourceMap.put(RF, source.get(RF).toString());

		source.clear();
		return sourceMap;
	}
}
