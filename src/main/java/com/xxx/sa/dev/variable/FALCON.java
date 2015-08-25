package com.xxx.sa.dev.variable;

import java.io.Serializable;

public class FALCON  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 440022861029617995L;
	public FALCON(){}
	 public FALCON(String tags){
		this.tags = tags;
	}
	
	public String getEndpoint() {
		return endpoint;
	}
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	public String getMetric() {
		return metric;
	}
	public void setMetric(String metric) {
		this.metric = metric;
	}
	public int getStep() {
		return step;
	}
	public void setStep(int step) {
		this.step = step;
	}
	public long getValue() {
		return value;
	}
	public void setValue(long value) {
		this.value = value;
	}
	public String getCounterType() {
		return counterType;
	}
	public void setCounterType(String counterType) {
		this.counterType = counterType;
	}
	public String getTags() {
		return tags;
	}
	public void setTags(String tags) {
		this.tags = tags;
	}
	String endpoint="uaq-storm";
	String metric;
	long timestamp;
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	int step=60;
	long value;
	
	String counterType="GAUGE";
	String tags;
}
