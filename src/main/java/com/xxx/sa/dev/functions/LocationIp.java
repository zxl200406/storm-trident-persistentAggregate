package com.xxx.sa.dev.functions;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

import com.xxx.sa.dev.common.Common;
import com.xxx.sa.dev.variable.DATA;

import storm.trident.operation.Aggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class LocationIp implements Aggregator<Map<String, DATA>> {
	private static final long serialVersionUID = 5539933633138675769L;

	public void aggregate(Map<String, DATA> map,TridentTuple tuple, TridentCollector collector) {
		long time=tuple.getLong(0);
		String src_province=tuple.getString(1);
		String Ip=tuple.getString(2);
		//"rx_pkgs","rx_bytes","tx_pkgs","tx_bytes"
		DATA  data1=(DATA) tuple.get(3);
		

	
		
		
		
		src_province="area="+Common.GetProvinceIndex(src_province);
		Ip="ip="+Ip;
		String key=src_province+","+Ip+Common.GetSymbol()+time;

		
		
		//如果这个map存在，key就累加
		if(map.containsKey(key)){
			DATA data=map.get(key);
			data.Add(data1);	

		}else{
			map.put(key, data1);
		}
	}



	public void complete(Map<String, DATA> map,TridentCollector collector) {
		if (map.size() > 0) {
			for(Entry<String, DATA> entry : map.entrySet()){
				collector.emit(new Values(entry.getKey(),entry.getValue()));
			}
			System.gc();
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
