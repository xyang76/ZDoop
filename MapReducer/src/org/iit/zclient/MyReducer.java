package org.iit.zclient;

import org.iit.zdoop.Context;
import org.iit.zdoop.Reducer;

public class MyReducer extends Reducer {

	@Override
	public void reduce(Object key, Integer[] values, Context context) {
		int sum = 0;
		for (int val : values) {
			sum += val; // Here we should add the sum from each partition
						// instead of 1.
		}
		context.write(String.valueOf(key), sum);
	}

}
