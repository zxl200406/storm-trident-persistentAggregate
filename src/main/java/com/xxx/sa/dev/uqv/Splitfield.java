package com.xxx.sa.dev.uqv;

import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.exception.ExceptionUtils;

import backtype.storm.tuple.Values;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xxx.sa.dev.variable.DATA;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class Splitfield extends BaseFunction {


	private static final long serialVersionUID = 8428147000997859495L;
	@Override
	public void execute(TridentTuple input, TridentCollector collector) {
		String source=input.getString(0).toLowerCase();
	//	System.out.println(source);
		try{
			JSONObject object= JSON.parseObject(source);
			
			String srcip=object.getString("srcip");
			String dstip=object.getString("dstip");
			int src_port=object.getInteger("src_port");//源端口
			int dst_port=object.getInteger("dst_port");//源端口
			String protocal=object.getString("protocol");//协议
			
			//新增字段
			//end 新增字段
			
			long rx_pkgs=object.getLong("rx_pkgs");//
			long rx_bytes=object.getLong("rx_bytes");//
			long tx_pkgs=object.getLong("tx_pkgs");//;//
			long tx_bytes=object.getLong("tx_bytes");//;//

			String src_country=object.getString("src_country");//;//;//国家
			String src_province=object.getString("src_province");//;//;//国家
			String src_city=object.getString("src_city");//;//;//国家
			String src_isp=object.getString("src_isp");//;//;//国家

			long time=object.getLong("time");
			
			time=time-time%60;//这里直接取消掉time的秒。所有的秒都是0
			
	    	
	    	
			
			long code=object.getInteger("code");
			
			long ttc=object.getInteger("ttc");
			if(ttc==-1){
				return;
			}
			long thc=object.getInteger("thc");
			if(thc==-1){
				return;
			}
			long thr=object.getInteger("thr");
			if(thr==-1){
				return;
			}
			long rrt=object.getInteger("rrt");
			if(rrt==-1){
				return;
			}
			
			
			
			
			//为结构体赋值
			DATA data=new DATA();
			data.setRx_bytes(rx_bytes);
			data.setRx_pkgs(rx_pkgs);
			data.setTx_bytes(tx_bytes);
			data.setTx_pkgs(tx_pkgs);
			
			data.setCounter(1);
			
			data.setTtc(ttc);
			data.setThc(thc);
			data.setThr(thr);
			data.setRrt(rrt);
			
			
			//新增赋值
			if(code==200){
				data.setCode(1);
				
			}else{
				data.setCodeNone(1);
			}
			//end 新增赋值
			
			collector.emit(new Values(time,srcip,dstip,src_port,dst_port,protocal,data,src_country,src_province,src_city,src_isp));



			
			
		}catch(Exception e){
			System.out.println("Splitjava ="+ExceptionUtils.getFullStackTrace(e));;
		}
}






}