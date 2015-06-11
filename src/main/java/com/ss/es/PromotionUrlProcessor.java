package com.ss.es;

import com.alibaba.fastjson.JSON;
import com.ss.main.Constants;
import com.ss.parser.KeywordExtractor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by dolphineor on 2015-6-9.
 * <p>
 * 推广URL统计信息处理器
 */
@SuppressWarnings("unchecked")
class PromotionUrlProcessor implements Constants {

    public static Map<String, Object> handle(Map<String, Object> source) {
        if (source.isEmpty())
            return Collections.emptyMap();

        String location = source.get(CURR_ADDRESS).toString();
        String ut = source.remove(UT).toString();
        Map<String, Object> utMap = new HashMap<>();

        // 提取推广URL统计信息
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) JSON.parse(ut)).entrySet()) {
            utMap.put(entry.getKey(), entry.getValue());
        }

        // 提取关键词信息
        Map<String, Object> keywordInfoMap = KeywordExtractor.parse(location);
        if (!keywordInfoMap.isEmpty())
            source.putAll(keywordInfoMap);

        utMap.put(INDEX, source.get(INDEX).toString());
        utMap.put(TYPE, source.get(TYPE).toString());
        utMap.put(TT, source.get(TT).toString());
        utMap.put(VID, source.get(VID).toString());
        utMap.put(CURR_ADDRESS, location);
        utMap.put(UNIX_TIME, source.get(UNIX_TIME).toString());

        source.clear();
        return utMap;
    }
}
