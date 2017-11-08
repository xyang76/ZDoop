package org.iit.zdoop;

import java.io.*;

/*
 * Job is a serializable class that can store the mapper, reducer and data. 
 */
public class Job implements Serializable {
	/**
	 * Parameters
	 */
	private static final long serialVersionUID = 1L;
	private String mapper;
	private String reducer;
	private byte[] mapperData;
	private byte[] reducerData;
	private byte[] data;
	private int status;
	private int jobid;
	private String name; 

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


	public void setStatus(int status) {
		this.status = status;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}
	
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMapper() {
		return mapper;
	}

	public void setMapper(Class<?> mapper) {
		this.mapper = mapper.getName();
	}

	public String getReducer() {
		return reducer;
	}

	public void setReducer(Class<?> reducer) {
		this.reducer = reducer.getName();
	}
	
	public void setMapper(String mapper) {
		this.mapper = mapper;
	}

	public void setReducer(String reducer) {
		this.reducer = reducer;
	}
}
