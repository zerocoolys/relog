package com.zyx.storm.topologies.function;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.json.simple.JSONObject;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.zyx.log.RelogLog;
import com.zyx.main.Constants;
import com.zyx.parser.GarbledCodeParser;
import com.zyx.parser.KeywordExtractor;
import com.zyx.parser.SearchEngineParser;
import com.zyx.redis.JRedisPools;
import com.zyx.utils.GaDateUtils;
import com.zyx.utils.PandantTools;
import com.zyx.utils.UrlUtils;

import backtype.storm.tuple.Values;
import net.sf.json.JSONArray;
import redis.clients.jedis.Jedis;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

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
		Jedis jedis =null;
		String receivedStr = null;
		try {
			 receivedStr = tuple.getString(0);
			Map<String, Object> mapSource = PandantTools.parseJSON2Map(receivedStr);
			
			 jedis = JRedisPools.getConnection();
			String trackId = mapSource.getOrDefault(T, EMPTY_STRING).toString();
			mapSource.remove(T);
			
			esType = jedis.get(TYPE_ID_PREFIX + trackId);
			

			long time = Long.parseLong(mapSource.get(UNIX_TIME).toString());
			Calendar calendar = Calendar.getInstance();
			calendar.setTimeInMillis(time);
			String dateString = DATE_FORMAT.format(calendar.getTime());
			String ipDupliKey = trackId + DELIMITER + dateString;
			long statusCode = jedis.sadd(ipDupliKey, mapSource.get(REMOTE).toString());
			mapSource.put(IP_DUPLICATE, statusCode);
			// 设置过期时间
			jedis.expire(ipDupliKey, ONE_DAY_SECONDS + 3600);

			// 页面转化
//		pageConversionProcessor.add(mapSource, esType);
			
			
			
			// 区分普通访问, 事件跟踪, xy坐标, 推广URL, 指定广告跟踪统计信息
			String eventInfo = mapSource.getOrDefault(ET, EMPTY_STRING).toString();
			String xyCoordinateInfo = mapSource.getOrDefault(XY, EMPTY_STRING).toString();
			String promotionUrlInfo = mapSource.getOrDefault(UT, EMPTY_STRING).toString();
			String adTrackInfo = mapSource.getOrDefault(AD_TRACK, EMPTY_STRING).toString();
			String pcname = mapSource.getOrDefault(PAGE_CONVERSION_NAME, EMPTY_STRING).toString();//页面转化
			String siteUrl = jedis.get(SITE_URL_PREFIX + trackId);
			
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
			    Map<String, Object> eventmap =  eventHandle(mapSource, jedis.get(tt), "siteUrl");
			    Values events = new Values(mapSource.get("index").toString(),mapSource.get("TYPE").toString(),PandantTools.nextCode(),JSONObject.toJSONString(eventmap));
			    collector.emit(events);
			    mapSource=null;
			
			}else if(!xyCoordinateInfo.isEmpty()){
				  mapSource.put(TYPE, esType + ES_TYPE_XY_SUFFIX);
				  Map<String, Object> xyCoordinateInfomap =  xyCoordinateInfoHandle(mapSource);
				  collector.emit(new Values(mapSource.get("index").toString(),mapSource.get("TYPE").toString(),PandantTools.nextCode(),JSONObject.toJSONString(xyCoordinateInfomap)));
				  mapSource=null;
			}else if (!promotionUrlInfo.isEmpty()) {
                if (!mapSource.get(CURR_ADDRESS).toString().contains(SEM_KEYWORD_IDENTIFIER)){
                	  mapSource=null;
                }else{
                	 mapSource.put(TYPE, esType + ES_TYPE_PROMOTION_URL_SUFFIX);
                     mapSource.put(HOST, trackId);
                     Map<String, Object> promotionUrlInfomap =  promotionUrlInfoHandle(mapSource);
     				collector.emit(new Values(mapSource.get("index").toString(),mapSource.get("TYPE").toString(),PandantTools.nextCode(),JSONObject.toJSONString(promotionUrlInfomap)));
     				  mapSource=null;
                }
            }else if (!adTrackInfo.isEmpty()) {
                adTrackMap.put(INDEX, mapSource.get(INDEX).toString());
                adTrackMap.put(TYPE, esType + ES_TYPE_AD_TRACK);
                adTrackMap.put(AD_SOURCE, mapSource.get(AD_SOURCE).toString());
                adTrackMap.put(AD_MEDIA, mapSource.get(AD_MEDIA).toString());
                adTrackMap.put(AD_CAMPAIGN, mapSource.get(AD_CAMPAIGN).toString());
                adTrackMap.put(AD_KEYWORD, mapSource.get(AD_KEYWORD).toString());
                adTrackMap.put(AD_CREATIVE, mapSource.get(AD_CREATIVE).toString());
                adTrackMap.put(REMOTE, mapSource.get(REMOTE).toString());
                adTrackMap.put(UNIX_TIME, Long.parseLong(mapSource.get(UNIX_TIME).toString()));
                String ipAdsDupliKey = trackId + ":ads" + DELIMITER + dateString;
                long adSstatusCode = jedis.sadd(ipAdsDupliKey, mapSource.get(REMOTE).toString());
                jedis.expire(ipAdsDupliKey, ONE_DAY_SECONDS + 3600);
                adTrackMap.put(IP_DUPLICATE, adSstatusCode);

            }
			
			
			
			 mapSource.put(TYPE, esType);
			 // 检测是否是一次的新的访问(1->新的访问, 0->同一次访问)
             int identifier = Integer.valueOf(mapSource.getOrDefault(NEW_VISIT, 0).toString());
             String locTemp = mapSource.get(CURR_ADDRESS).toString();
             int lastIndex = locTemp.length() - 1;
             if (Objects.equals('/', locTemp.charAt(lastIndex))) {
                 locTemp = locTemp.substring(0, lastIndex);
                 mapSource.put(CURR_ADDRESS, locTemp);
                 locTemp = null;
             }
			
             if (identifier == 1) {
                 mapSource.put(ENTRANCE, 1);
                 mapSource.remove(NEW_VISIT);
                 String _location = mapSource.get(CURR_ADDRESS).toString();

                 boolean hasPromotion = false;
                 if (_location.contains(SEM_KEYWORD_IDENTIFIER)) {
                     // keyword extract
                     Map<String, Object> keywordInfoMap = KeywordExtractor.parse(_location);
                     if (!keywordInfoMap.isEmpty())
                         mapSource.putAll(keywordInfoMap);

                     hasPromotion = true;
                 }

                 mapSource.put(DESTINATION_URL, hasPromotion ? _location : PLACEHOLDER);
             } else {
                 mapSource.put(ENTRANCE, 0);
             }
			
             // 来源类型解析
             String refer = mapSource.get(RF).toString();
             String rf_type=VAL_RF_TYPE_OUTLINK;
             if (PLACEHOLDER.equals(refer) || UrlUtils.match(siteUrl, refer)) {  // 直接访问
                 mapSource.put(RF, PLACEHOLDER);
                 mapSource.put(SE, PLACEHOLDER);
                 mapSource.put(KW, PLACEHOLDER);

                 rf_type = jedis.get(tt);
                 if (rf_type == null) {
                     mapSource.put(RF_TYPE, VAL_RF_TYPE_DIRECT);
                     jedis.setex(tt, ONE_DAY_SECONDS, VAL_RF_TYPE_DIRECT);
                 } else {
                     mapSource.put(RF_TYPE, rf_type);
                 }

                 mapSource.put(DOMAIN, PLACEHOLDER);
             } else {
                 List<String> skList = Lists.newArrayList();
                 boolean found = SearchEngineParser.getSK(java.net.URLDecoder.decode(refer, StandardCharsets.UTF_8.name()), skList);
                 // extract domain from rf
                 URL url = new URL(refer);
                 mapSource.put(DOMAIN, url.getProtocol() + DOUBLE_SLASH + url.getHost());
                 if (found) {    // 搜索引擎
                     mapSource.put(SE, skList.remove(0));

                     // 搜索词乱码识别
                     String searchWord = skList.remove(0);
                     if (GarbledCodeParser.isGarbledCode(searchWord))
                         searchWord = GARBLED_VALUE;
                     mapSource.put(KW, searchWord);

                     rf_type = jedis.get(tt);
                     if (rf_type == null) {
                         mapSource.put(RF_TYPE, VAL_RF_TYPE_SE);
                         jedis.setex(tt, ONE_DAY_SECONDS, VAL_RF_TYPE_SE);
                     } else {
                         mapSource.put(RF_TYPE, rf_type);
                     }
                 } else {    // 外部链接
                     rf_type = jedis.get(tt);
                     if (rf_type == null) {
                         mapSource.put(RF_TYPE, VAL_RF_TYPE_OUTLINK);
                         jedis.setex(tt, ONE_DAY_SECONDS, VAL_RF_TYPE_OUTLINK);
                     } else {
                         mapSource.put(RF_TYPE, rf_type);
                     }
                 }
             }
			
             
             // 访问URL路径解析
             String location = UrlUtils.removeProtocol(mapSource.get(CURR_ADDRESS).toString());
             if (location.contains(QUESTION_MARK))
                 location = location.substring(0, location.indexOf(QUESTION_MARK));

             Map<String, String> pathMap = new HashMap<>();

             final AtomicInteger integer = new AtomicInteger(0);
             Consumer<String> pathConsumer = (String c) -> pathMap.put(HTTP_PATH + (integer.getAndIncrement()), c);
             Arrays.asList(location.split("/")).stream().filter((p) -> !p.isEmpty() || !p.startsWith(HTTP_PREFIX)).forEach(pathConsumer);
             mapSource.put(PATHS, pathMap);
             
             
             if (!adTrackInfo.isEmpty()) {
                 fillingDate(mapSource, adTrackMap);
  				collector.emit(new Values(adTrackMap.get("index").toString(),adTrackMap.get("TYPE").toString(),PandantTools.nextCode(),JSONObject.toJSONString(adTrackMap)));
             }
             if (!pcname.isEmpty()) {
                 mapSource.put(TYPE, esType + ES_TYPE_PAGE_SUFFIX);
             }
			
			if(mapSource!=null){
				collector.emit(new Values(mapSource.get("index").toString(),mapSource.get(TYPE).toString(),PandantTools.nextCode(),JSONObject.toJSONString(mapSource)));
				 System.out.println("********************"+JSONObject.toJSONString(mapSource));
			}

		}  catch (Exception e) {
			e.printStackTrace();
			RelogLog.record(e.getMessage() + "[  "+receivedStr+"   ]");
		} finally {
            if (jedis != null) {
                jedis.close();
            }
        }

		


	
	}
	
	
	private Map<String, Object> promotionUrlInfoHandle(Map<String, Object> source) {
		 if (source.isEmpty())
	            return Collections.emptyMap();

	        String location = source.get(CURR_ADDRESS).toString();
	        String ut = source.remove(UT).toString();
	        Map<String, Object> promotionUrlMap = new HashMap<>();

	        // 提取推广URL统计信息
	        for (Map.Entry<String, Object> entry : ((Map<String, Object>) JSON.parse(ut)).entrySet()) {
	            promotionUrlMap.put(entry.getKey(), entry.getValue());
	        }

	        // 提取关键词信息
	        Map<String, Object> keywordInfoMap = KeywordExtractor.parse(location);
	        if (!keywordInfoMap.isEmpty())
	            source.putAll(keywordInfoMap);

	        promotionUrlMap.put(INDEX, source.get(INDEX).toString());
	        promotionUrlMap.put(TYPE, source.get(TYPE).toString());
	        promotionUrlMap.put(HOST, source.get(HOST).toString()); // 存储的是网站的trackId
	        promotionUrlMap.put(TT, source.get(TT).toString());
	        promotionUrlMap.put(VID, source.get(VID).toString());
	        promotionUrlMap.put(CURR_ADDRESS, location);
	        promotionUrlMap.put(REGION, source.get(REGION).toString());
	        promotionUrlMap.put(CITY, source.get(CITY).toString());
	        promotionUrlMap.put(ISP, source.get(ISP).toString());

	        source.clear();
	        return promotionUrlMap;
	}

	private Map<String, Object> xyCoordinateInfoHandle(Map<String, Object> source) {
		 if (source.isEmpty())
	            return Collections.emptyMap();

	        Map<String, Object> sourceMap = new HashMap<>();

	        // 解析xy数组
	        JSONArray xyJsonArray = (JSONArray) JSON.parse(source.get(XY).toString());
	        Object[] xyArr = xyJsonArray.stream().map(xy -> (Map<String, Double>) xy).toArray();

	        sourceMap.put(INDEX, source.get(INDEX).toString());
	        sourceMap.put(TYPE, source.get(TYPE).toString());
	        sourceMap.put(TT, source.get(TT).toString());
	        sourceMap.put(VID, source.get(VID).toString());
	        sourceMap.put(CURR_ADDRESS, source.get(CURR_ADDRESS).toString());
	        sourceMap.put(XY, xyArr);
	        sourceMap.put(DH, source.get(DH));
	        source.clear();
	        return sourceMap;
	}

	private  Map<String, Object> eventHandle(Map<String, Object> source,	String rf_type, String siteUrl) throws UnsupportedEncodingException {
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
	
	   private void fillingDate(Map<String, Object> mapSource, Map<String, Object> adTrackMap) {
           adTrackMap.put(TT, mapSource.get(TT).toString());

           if (mapSource.containsKey(AD_RF)) {
               adTrackMap.put(RF, mapSource.get(AD_RF).toString());
               adTrackMap.put(AD_RF, mapSource.get(AD_RF).toString());
           } else {
               Map<String, String> params = URLRequest(mapSource.get(CURR_ADDRESS).toString());
               if (params.containsKey(AD_RF)) {
                   adTrackMap.put(RF, params.get(AD_RF));
               } else {
                   adTrackMap.put(RF, PLACEHOLDER);
               }

           }
           if (mapSource.containsKey(CURR_ADDRESS)) {
               adTrackMap.put(CURR_ADDRESS, mapSource.get(CURR_ADDRESS).toString());
           }

           if (mapSource.containsKey(UCV)) {
               adTrackMap.put(UCV, mapSource.get(UCV).toString());
           }

           if (mapSource.containsKey(CITY)) {
               adTrackMap.put(CITY, mapSource.get(CITY).toString());
           }

           if (mapSource.containsKey(ISP)) {
               adTrackMap.put(ISP, mapSource.get(ISP).toString());
           }

           if (mapSource.containsKey(VID)) {
               adTrackMap.put(VID, mapSource.get(VID).toString());
           }

           if (mapSource.containsKey(SE)) {
               adTrackMap.put(SE, mapSource.get(SE).toString());
           }
           if (mapSource.containsKey(AD_TRACK)) {
               adTrackMap.put(AD_TRACK, mapSource.get(AD_TRACK).toString());
           }
           if (mapSource.containsKey(CLIENT_TIME)) {
               adTrackMap.put(CLIENT_TIME, mapSource.get(CLIENT_TIME).toString());
           }
           if (mapSource.containsKey(ENTRANCE)) {
               adTrackMap.put(ENTRANCE, mapSource.get(ENTRANCE).toString());
           }
           if (mapSource.containsKey(RF_TYPE)) {
               adTrackMap.put(RF_TYPE, mapSource.get(RF_TYPE).toString());
           }
           if (mapSource.containsKey(HOST)) {
               adTrackMap.put(HOST, mapSource.get(HOST).toString());
           }
           if (mapSource.containsKey(KW)) {
               adTrackMap.put(KW, mapSource.get(KW).toString());
           }
           if (mapSource.containsKey(VERSION)) {
               adTrackMap.put(VERSION, mapSource.get(VERSION).toString());
           }
           if (mapSource.containsKey(METHOD)) {
               adTrackMap.put(METHOD, mapSource.get(METHOD).toString());
           }
           if (mapSource.containsKey(VISITOR_IDENTIFIER)) {
               adTrackMap.put(VISITOR_IDENTIFIER, mapSource.get(VISITOR_IDENTIFIER).toString());
           }

           if (mapSource.containsKey(REGION)) {
               adTrackMap.put(REGION, mapSource.get(REGION).toString());
           }
           if (mapSource.containsKey(DOMAIN)) {
               adTrackMap.put(DOMAIN, mapSource.get(DOMAIN).toString());
           }
           if (mapSource.containsKey(ENTRANCE)) {
               adTrackMap.put(ENTRANCE, mapSource.get(ENTRANCE).toString());
           }
           if (PLACEHOLDER.equals(adTrackMap.get(RF).toString().trim())) {
               adTrackMap.put(RF, "直接访问");
           }
       }
	   
	   
	   
	   /**
        * @param URL
        * @description 解析url获取参数
        * @author ZhangHuaRong
        * @update 2015年10月28日 下午2:19:24
        */
       private Map<String, String> URLRequest(String URL) {
           Map<String, String> mapRequest = new HashMap<String, String>();
           String[] arrSplit = null;
           String strUrlParam = TruncateUrlPage(URL);
           if (strUrlParam == null) {
               return mapRequest;
           }
           // 每个键值为一组
           arrSplit = strUrlParam.split("[&]");
           for (String strSplit : arrSplit) {
               String[] arrSplitEqual = null;
               arrSplitEqual = strSplit.split("[=]");
               // 解析出键值
               if (arrSplitEqual.length > 1) {
                   // 正确解析
                   mapRequest.put(arrSplitEqual[0], arrSplitEqual[1]);
               } else {
                   if (arrSplitEqual[0] != "") {
                       // 只有参数没有值，不加入
                       mapRequest.put(arrSplitEqual[0], "");
                   }
               }
           }
           return mapRequest;
       }
       private String TruncateUrlPage(String strURL) {
           String strAllParam = null;
           String[] arrSplit = null;
           strURL = strURL.trim().toLowerCase();
           arrSplit = strURL.split("[?]");
           if (strURL.length() > 1) {
               if (arrSplit.length > 1) {
                   if (arrSplit[1] != null) {
                       strAllParam = arrSplit[1];
                   }
               }
           }
           return strAllParam;
       }

}
