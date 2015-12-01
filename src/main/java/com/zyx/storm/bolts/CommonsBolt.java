package com.zyx.storm.bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by yousheng on 15/12/1.
 */
public class CommonsBolt extends BaseRichBolt {
    private OutputCollector _outputCollector;

    private Logger logger = LoggerFactory.getLogger(CommonsBolt.class);

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this._outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(tuple.toString());
            }

            _outputCollector.ack(tuple);
        } catch (Exception ex) {
            _outputCollector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
