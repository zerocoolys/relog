package com.zyx.storm.topologies.function;

import backtype.storm.tuple.Values;
import org.json.simple.JSONObject;

import com.zyx.utils.PandantTools;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;
import java.util.Random;


public class PrepareForESTest extends BaseFunction {

    private String esIndex,esType,esTypeads;

    public void prepare(Map conf, TridentOperationContext context) {
        this.esIndex ="access-2015-11-19";
        this.esType = "557c060c767a452c125ec058_event";
        this.esTypeads = "557c060c767a452c125ec058_ads";
    }

  /*  public void execute(TridentTuple tuple, TridentCollector collector) {

        JSONObject json = new JSONObject();
        json.put("source", tuple.getString(0));

        Random rand = new Random();
        Integer id =  Math.abs(rand.nextInt());
        collector.emit(new Values(esIndex,esType,id.toString(),json.toJSONString()));
    }*/
    
    
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	String str =tuple.getString(0);
    	Map<String, Object> mapSource = PandantTools.parseJSON2Map(str);
    	  JSONObject json = new JSONObject();
    	  
    	  for (String key : mapSource.keySet()) {
    		   json.put(key, mapSource.get(key));
    	  }
    	  
    	 String index= mapSource.get("index").toString();
    	 String type  = "222bf2d17cffad63c25ec1528864ca11";
    	 
    	  
    	  System.out.println("********************"+json.toJSONString());
//        collector.emit(new Values(mapSource.get("index"),mapSource.get("TYPE"),PandantTools.nextCode(),json.toJSONString()));
    	  collector.emit(new Values(esIndex,esType,PandantTools.nextCode(),json.toJSONString()));
    	  collector.emit(new Values(esIndex,type+"_ads",PandantTools.nextCode(),json.toJSONString()));
    }
}
