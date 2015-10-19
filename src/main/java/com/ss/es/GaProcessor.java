package com.ss.es;

import java.text.ParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.DBObject;
import com.ss.main.Constants;
import com.ss.mongo.MongoDBUtil;
import com.ss.utils.GaDateUtils;

public class GaProcessor implements Constants {

	private final BlockingQueue<Map<String, Object>> gaQueue = new LinkedBlockingQueue<>();

	GaProcessor() {

		Thread requestThread = new Thread(() -> {
			while (true) {
				Map<String, Object> gaMap = null;
				try {
					gaMap = gaQueue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				if (gaMap == null)
					continue;
				// 保存数据
				saveDate(gaMap);
			}
		});

		requestThread.start();
	}

	private void saveDate(Map<String, Object> source) {

		if (source.size() == 0) {
			return;
		}

		String type = source.get(TYPE).toString();
		String userId = source.get(VID).toString();

		Map<String, Object> querier = new HashMap<String, Object>();
		querier.put(MONGODB_TYPE, type);
		querier.put(MONGODB_USER_ID, userId);

		// 保存数据----》每日记录
		String collName = getDayOfCollName();
		saveData(collName, querier);
		// 保存数据----》每周记录
		collName = getWeekOfCollName();
		saveData(collName, querier);
		// 保存数据----》每月记录
		collName = getMonthOfCollName();
		saveData(collName, querier);

	}

	public void add(Map<String, Object> source) {
		Map<String, Object> map = new HashMap<>(source);
		gaQueue.add(map);
	}

	public String getDayOfCollName() {

		String dateString = GaDateUtils.getCurrentDate();
		String collName = MONGODB_PREFIX + dateString;
		return collName;
	}

	public String getWeekOfCollName() {

		String dateString = GaDateUtils.getWeekMonday();
		String collName = MONGODB_PREFIX_WEEK + dateString;
		return collName;
	}

	public String getMonthOfCollName() {

		String dateString = GaDateUtils.getCurrentMonth();
		String collName = MONGODB_PREFIX_MONTH + dateString;
		return collName;
	}

	public void saveData(String collName, Map<String, Object> querier) {

		DBObject dBObject = MongoDBUtil.findOne(querier, collName);

		if (dBObject != null) {
			Integer pv = Integer.valueOf(dBObject.get(MONGODB_PV).toString()) + 1;
			dBObject.put(MONGODB_PV, pv);
			MongoDBUtil.update(MongoDBUtil.getMapped(querier), dBObject, false,
					false, collName);
		} else {
			querier.put(MONGODB_IS_NEW, NEW_CUSTOMER);
			querier.put(MONGODB_PV, 1);
			dBObject = MongoDBUtil.getMapped(querier);

			MongoDBUtil.insert(dBObject, collName);
		}
	}
	
	


}
