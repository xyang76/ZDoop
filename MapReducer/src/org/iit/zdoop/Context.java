package org.iit.zdoop;

import java.util.HashMap;

public class Context {
	
	private HashMap<String, Integer> context;
	
	public Context() {
		this.context = new HashMap<>();
	}
	
	public void write(String key, int i) {
		Integer iw = this.context.get(key);
		if(iw == null) {
			this.context.put(key, i);
		} else {
			this.context.put(key, i + iw);
		}
	}
	
	
	public HashMap<String, Integer> getResult() {
		return context;
	}
}
