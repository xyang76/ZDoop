package org.iit.zdoop;

import java.io.Serializable;

public class Task implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Class<?> mapper;
	private Class<?> reducer;
	private byte[] data;
	private int status;
	private int taskid;
	private int jobid;
	
	public int getJobid() {
		return jobid;
	}

	public void setJobid(int jobid) {
		this.jobid = jobid;
	}

	public int getTaskid() {
		return taskid;
	}

	public void setTaskid(int taskid) {
		this.taskid = taskid;
	}

	public Task() {
		this.status = 0;			
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
