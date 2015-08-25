package com.xxx.sa.dev.memdb;


import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.alibaba.fastjson.JSON;
import com.xxx.sa.dev.common.Common;
import com.xxx.sa.dev.variable.DATA;
import com.xxx.sa.dev.variable.FALCON;



public class DB implements Serializable  {
	private static final long serialVersionUID = -5694732161515374879L;


	private int selfNum=0;


	private  DateFormat temp_Df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	private final int MapLength=3;//定义map的长度为3
	private  TreeMap<String,Map<String,DATA>> map=new TreeMap<String,Map<String,DATA>>();//存放数据的地方

	public  DB(int selfNum){
		this.selfNum=selfNum;
	}

	public void BatchAdd(DB olddb){	
		if(this.map.size()>0){							
			//遍历当前批次的数据
			for(Entry<String, Map<String, DATA>> entry : this.map.entrySet()) {
				String key=entry.getKey();

				//外层key存在不存在的情况下，并new出来和新的同步
				if(!olddb.map.containsKey(key)){
					Map<String, DATA> map_old_new=new HashMap<String, DATA>();
					olddb.map.put(key, map_old_new);
				}

				Map<String, DATA> map_new=entry.getValue();
				Map<String, DATA> map_old=olddb.map.get(key);


				//遍历map_new，并插入到map_old
				for(Entry<String, DATA> s: map_new.entrySet()){
					DATA d1=map_new.get(s.getKey());
					DATA d2=map_old.get(s.getKey());
					if(d2==null){
						d2=new DATA();
						d2.Add(d1);
						map_old.put(s.getKey(), d2);
					}else{
						d2.Add(d1);
					}
				}//for(Entry<String, DATA> s: map_new.entrySet())

			}//for(Entry<String, Map<String, DATA>> entry : this.map.entrySet())	
		}//if

	}




	public void Init(String rowkey,DATA data){
		try{
			String arr[]=StringUtils.split(rowkey,Common.GetSymbol());
			long time=Long.parseLong(arr[arr.length-1]);
			Date temp_Date=new Date(time*1000);
			String key=temp_Df.format(temp_Date);


			if(this.map.containsKey(key)){
				Map<String,DATA> inner_map=this.map.get(key);
				inner_map.put(rowkey, data);		
			}else{
				Map<String,DATA> inner_map=new HashMap<String,DATA>();
				inner_map.put(rowkey, data);
				this.map.put(key, inner_map);
			}
		}catch(Exception e){
			ExceptionUtils.getFullStackTrace(e);
		}
	}



	public BlockingQueue<String> GetData(){	
		int size=map.size();

		if(size>0){
			System.out.println("host:="+Common.GetHostName()+"|mem="+Common.GetMemSize()+"|map.size()="+map.size()+"|last key="+map.lastKey()+"即最新对应的数据size()="+map.get(map.lastKey()).size()+"|first key="+map.firstKey()+"即最老的对应的数据size()="+map.get(map.firstKey()).size());
		}

		if(size<MapLength){
			return null;
		}//if

		BlockingQueue<String> queue=new LinkedBlockingQueue<String>();;
		String key=map.firstKey();


		Map<String,DATA> temp_data=map.get(key);
		makeDATA(queue,temp_data);


		System.out.println("我是功能"+selfNum);
		



		System.out.println("temp_data.size()="+temp_data.size());

		//释放内存
		temp_data.clear();
		map.remove(key);

		if(queue.size()==0){
			System.out.println("queue.size()=0");
			return null;
		}else{
			System.out.println("queue.size()="+size);
		}
		System.gc();


		return queue;
	}
	private void makeDATA(BlockingQueue<String> queue,Map<String,DATA> temp_data){

		if(temp_data.size()<=0){
			return;
		}
		int counter=0;

		List<FALCON> list=new ArrayList<FALCON>();
		String src_key="";

		for(Entry<String, DATA> entry : temp_data.entrySet()){		
			counter++;		
			String arr[]=StringUtils.split(entry.getKey(),Common.GetSymbol());
			if(src_key==""){
				src_key=entry.getKey();

			}
			String tags=arr[0];
			long time=Long.parseLong(arr[1]);
			DATA data=entry.getValue();
			data.Average();//求平均，只能转falcon的时间调用。


			try {			
				Field[] field = data.getClass().getDeclaredFields(); 
				for(int j=0 ; j<field.length ; j++){     //遍历所有属性
					String name = field[j].getName();    //获取属性的名字
					if(name.endsWith("serialVersionUID")){
						continue;
					}
					Method m = data.getClass().getMethod("get"+name);
					Long value =(Long) m.invoke(data);    //调用getter方法获取属性值

					FALCON f=new FALCON(tags);	
					f.setValue(value);
					f.setTimestamp(time);
					f.setMetric(name.toLowerCase());



					list.add(f);
				}

			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {

			} 

			if(counter%100==0){
				String jsonstr=JSON.toJSONString(list);
				//System.out.println(jsonstr);
				queue.add(jsonstr);
				list.clear();
			}		
		}


		if(list.size()>0){
			String jsonstr=JSON.toJSONString(list);
			queue.add(jsonstr);

		}
		list.clear();


		System.out.println(src_key);
	}





}