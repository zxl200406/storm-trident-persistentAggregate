package com.xxx.sa.dev.uqv;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.xxx.sa.dev.memdb.DB;
import com.xxx.sa.dev.variable.DATA;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;




public  class Counter implements CombinerAggregator<DB> { 
	private int selfNum=0;
	@Override
	public DB init(TridentTuple tuple) {
		DB db = zero();

		try{
		String rowkey=tuple.getString(0);
		DATA  data=(DATA) tuple.get(1);
		db.Init(rowkey, data);
		}catch(Exception e){
			System.out.println("init()"+ExceptionUtils.getFullStackTrace(e));
		}
		return db;
		
	}
	@Override
	public DB combine(DB olddb, DB newdb) {
		
		newdb.BatchAdd(olddb);



		return olddb;
		
	}
	@Override
	public DB zero() {
		return new DB(selfNum);
	}
	
	public Counter(int selfNum){
		this.selfNum=selfNum;
	}


}