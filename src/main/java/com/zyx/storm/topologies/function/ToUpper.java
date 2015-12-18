package com.zyx.storm.topologies.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @title ToUpper.java
 * @package com.zyx.storm.topologies.function
 * @description TODO
 * @author ZhangHuaRong   
 * @update 2015年12月2日 下午3:16:14
 * @version V1.0  
 * Copyright (c)2012 chantsoft-版权所有
 */
public class ToUpper extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {

        collector.emit( new Values(tuple.getString(0).toUpperCase()));
    }
}
