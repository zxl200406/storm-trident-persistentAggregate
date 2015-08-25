package com.xxx.sa.dev.common;


import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import com.xxx.sa.dev.variable.DATA;



public class Common {

	private final static String symbol="$$";

	public static String GetSymbol(){
		return symbol;
	}

	public static String GetHostName(){
		try {
			InetAddress addr = InetAddress.getLocalHost();
			return addr.getHostName().toString();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
	}
	public static String GetMemSize(){
		int kb = 1024;
		long totalMemory = Runtime.getRuntime().totalMemory() / kb;
		// 剩余内存
		long freeMemory = Runtime.getRuntime().freeMemory() / kb;
		// 最大可使用内存
		long maxMemory = Runtime.getRuntime().maxMemory() / kb;
		return "totalMemory="+totalMemory+"|freeMemory="+freeMemory+"|maxMemory="+maxMemory;
	}


	private final static Map<String,Integer> mapProvince = new HashMap<String,Integer>(){
		private static final long serialVersionUID = -4084127291277778342L;
	{
		put("北京", 1);
		put("天津", 2);
		put("河北", 3);
		put("山西", 4);
		put("内蒙古",5);
		put("辽宁", 6);
		put("吉林", 7);
		put("黑龙江",8);
		put("上海", 9);
		put("江苏", 10);
		put("浙江", 11);
		put("安徽", 12);
		put("福建", 13);
		put("江西", 14);
		put("山东", 15);
		put("河南", 16);
		put("湖北", 17);
		put("湖南", 18);
		put("广西", 19);
		put("海南", 20);
		put("重庆", 21);
		put("四川", 22);
		put("贵州", 23);
		put("云南", 24);
		put("西藏", 25);
		put("陕西", 26);
		put("甘肃", 27);
		put("青海", 28);
		put("宁夏", 29);
		put("新疆", 30);
		put("广东", 31);
		put("香港", 32);
		put("澳门", 33);
		put("台湾", 34);
	}};


	private final static Map<String,Integer> mapOp = new HashMap<String,Integer>(){
		private static final long serialVersionUID = 2348105401543659686L;

	{
		put("联通", 1);
		put("电信", 2);
		put("移动", 3);
		put("网通", 4);
		put("教育网",5);
	}};

	public static int GetProvinceIndex(String Province){
		Integer index=mapProvince.get(Province);
		if(index==null){
			//不在列表
			return -1;
		}
		return index;
	}
	public static int GetOpIndex(String Op){
		Integer index=mapOp.get(Op);
		if(index==null){
			//不在列表
			return -1;
		}
		return index;
	}
	public static boolean IsNegative(DATA data){
	
		
		return true;
	}

}
