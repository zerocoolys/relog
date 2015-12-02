package com.zyx.storm.topologies.trident;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

/**
 * Created by yousheng on 15/12/1.
 */
public class FilterValidate extends BaseFilter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
        System.out.println("tuple.getValues() = " + tuple.getValues());
        return false;
    }
}
