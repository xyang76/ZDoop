package org.iit.zdoop;

import java.io.Serializable;

public class Task implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String mapper;
	private String reducer;
	private byte[] mapperData;
	private byte[] reducerData;
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
	

	public byte[] getMapperData() {
		return mapperData;
	}

	public void setMapperData(byte[] mapperData) {
		this.mapperData = mapperData;
	}

	public byte[] getReducerData() {
		return reducerData;
	}

	public void setReducerData(byte[] reducerData) {
		this.reducerData = reducerData;
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

	public String getMapper() {
		return mapper;
	}

	public void setMapper(String mapper) {
		this.mapper = mapper;
	}

	public String getReducer() {
		return reducer;
	}

	public void setReducer(String reducer) {
		this.reducer = reducer;
	}
}
