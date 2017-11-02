package org.iit.zdoop;

import java.io.Serializable;

public abstract class Reducer implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public abstract void reduce(Object key, Integer[] values, Context context);
}
