package com.xxx.sa.dev.variable;

import java.io.Serializable;

public class DATA implements Serializable  {


	public long getTtc() {
		return Ttc;
	}
	public void setTtc(long ttc) {
		Ttc = ttc;
	}
	public long getThc() {
		return Thc;
	}
	public void setThc(long thc) {
		Thc = thc;
	}
	public long getThr() {
		return Thr;
	}
	public void setThr(long thr) {
		Thr = thr;
	}
	public long getRrt() {
		return Rrt;
	}
	public void setRrt(long rrt) {
		Rrt = rrt;
	}
	public long getCode() {
		return Code;
	}
	public void setCode(long code) {
		Code = code;
	}
	public long getCodeNone() {
		return CodeNone;
	}
	public void setCodeNone(long codeNone) {
		CodeNone = codeNone;
	}
	public long getCounter() {
		return Counter;
	}
	public void setCounter(long counter) {
		this.Counter = counter;
	}

	public long getRx_pkgs() {
		return Rx_pkgs;
	}
	public void setRx_pkgs(long rx_pkgs) {
		Rx_pkgs = rx_pkgs;
	}
	public long getRx_bytes() {
		return Rx_bytes;
	}
	public void setRx_bytes(long rx_bytes) {
		Rx_bytes = rx_bytes;
	}
	public long getTx_pkgs() {
		return Tx_pkgs;
	}
	public void setTx_pkgs(long tx_pkgs) {
		Tx_pkgs = tx_pkgs;
	}
	public long getTx_bytes() {
		return Tx_bytes;
	}
	public void setTx_bytes(long tx_bytes) {
		Tx_bytes = tx_bytes;
	}

	
	public void Add(DATA datanew){
		Rx_bytes=this.getRx_bytes()+datanew.getRx_bytes();
		Rx_pkgs=this.getRx_pkgs()+datanew.getRx_pkgs();
		Tx_bytes=this.getTx_bytes()+datanew.getTx_bytes();
		Tx_pkgs=this.getTx_pkgs()+datanew.getTx_pkgs();		
		Counter=this.getCounter()+datanew.getCounter();
		
		Code=this.getCode()+datanew.getCode();
		CodeNone=this.getCodeNone()+datanew.getCodeNone();
		
		Ttc=this.getThc()+datanew.getThc();
		Thc=this.getTtc()+datanew.getThc();
		Thr=this.getThr()+datanew.getThr();
		Rrt=this.getRrt()+datanew.getRrt();
			
	}
	
	public void Average(){
		Ttc=Ttc/Counter;
		Thc=Thc/Counter;
		Thr=Thr/Counter;
		Rrt=Rrt/Counter;
	}
	
	
	
	long Ttc;
	long Thc;
	long Thr;
	long Rrt;
	
	long Rx_pkgs;//
	long Rx_bytes;//
	long Tx_pkgs;//
	long Tx_bytes;//
	
	
	long Code;//只记录200的情况，
	long CodeNone;//只记录非200的情况
	
	long Counter;//


}
