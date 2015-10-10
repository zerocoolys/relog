package com.ss.es;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


import com.mongodb.DBObject;
import com.ss.main.Constants;
import com.ss.mongo.MongoDBUtil;

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
				// 采集数据
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
		String isNew = source.get(VISITOR_IDENTIFIER).toString();
		String collName = getCollName();

		Map<String, Object> find = new HashMap<String, Object>();
		find.put(MONGODB_TYPE, type);
		find.put(MONGODB_USER_ID, userId);

		DBObject dBObject = MongoDBUtil.findOne(find, collName);

		if (dBObject != null) {
			Integer pv = Integer.valueOf(dBObject.get(MONGODB_PV).toString()) + 1;
			dBObject.put(MONGODB_PV, pv);
			MongoDBUtil.update(MongoDBUtil.getMapped(find), dBObject, false,
					false, collName);
		} else {
			find.put(MONGODB_IS_NEW, isNew);
			find.put(MONGODB_PV, 1);
			dBObject = MongoDBUtil.getMapped(find);

			MongoDBUtil.insert(dBObject, collName);
		}

	}

	public void add(Map<String, Object> source) {
		Map<String, Object> map = new HashMap<>(source);
		gaQueue.add(map);
	}

	public String getCollName() {

		long time = System.currentTimeMillis();
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(time);
		String dateString = DATE_FORMAT.format(calendar.getTime());

		String collName = MONGODB_PREFIX + dateString;
		return collName;
	}

}
