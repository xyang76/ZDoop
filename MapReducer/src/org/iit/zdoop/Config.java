package org.iit.zdoop;

import java.io.Serializable;
import java.util.ArrayList;

public class Config implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String server;
	private ArrayList<String> workers;
	
	public Config() {
		this.workers = new ArrayList<String>();
	}
	
	public String getServer() {
		return server;
	}

	public void setServer(String server) {
		this.server = server;
	}

	public ArrayList<String> getWorkers() {
		return workers;
	}

	public void setWorkers(ArrayList<String> workers) {
		this.workers = workers;
	}
}
