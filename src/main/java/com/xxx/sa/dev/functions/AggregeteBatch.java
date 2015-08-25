package com.xxx.sa.dev.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.tuple.Values;

import com.xxx.sa.dev.variable.DATA;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class AggregeteBatch implements Aggregator<Map<String, DATA>> {

	private static final long serialVersionUID = -1826655237812895865L;

	public void aggregate(Map<String, DATA> map,TridentTuple tuple, TridentCollector collector) {
		String key=tuple.getString(0);
		DATA  data1=(DATA) tuple.get(1);
		//如果这个map存在，key就累加
		if(map.containsKey(key)){
			DATA data=map.get(key);
			data.Add(data1);
			//批次间的counter数，累加
			map.put(key, data);
		}else{
			map.put(key, data1);
		}
	}



	public void complete(Map<String, DATA> map,TridentCollector collector) {
		if (map.size() > 0) {
			String text="";
			for(Entry<String, DATA> entry : map.entrySet()){
				if(text==""){
					text=entry.getKey();
				}
				collector.emit(new Values(entry.getKey(),entry.getValue()));
			}
			System.gc();
			System.out.println("first batch text="+text);
		} 
	}


	public Map<String, DATA> init(Object batchId,
			TridentCollector collector) {
		return new HashMap<String, DATA>();

	}

	public void cleanup() {
	}

	public void prepare(Map map, TridentOperationContext tridentOperationContext) {
		System.setProperty("java.security.krb5.conf",
		System.getProperty("user.dir") + "/krb5.conf");
	}

}
