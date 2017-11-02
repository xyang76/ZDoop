package org.iit.zdoop;

import java.io.Serializable;

public class KVPair implements Serializable {
	private String key;
	private int value;
	
	public KVPair(String key, Integer value) {
		this.key = key;
		this.value = value;
	}
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int getValue() {
		return value;
	}
	public void setValue(int value) {
		this.value = value;
	}
	
}
