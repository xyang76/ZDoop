package org.iit.zdoop;

import java.io.*;

public class Job implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Class<?> mapper;
	private Class<?> reducer;
	private byte[] data;
	private int status;
	private int jobid;
	
	public Job() {
		this.status = 0;			
	}
	
	public int getJobid() {
		return jobid;
	}

	public void setJobid(int jobid) {
		this.jobid = jobid;
	}
	
	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public Class<?> getMapper() {
		return mapper;
	}

	public void setMapper(Class<?> mapper) {
		this.mapper = mapper;
	}

	public Class<?> getReducer() {
		return reducer;
	}

	public void setReducer(Class<?> reducer) {
		this.reducer = reducer;
	}
}
