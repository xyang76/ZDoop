package org.iit.zdoop;

import java.io.Serializable;

public abstract class Mapper implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public abstract void map(Object key, Object value, Context context);
}
