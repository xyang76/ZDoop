package org.iit.zclient;

import org.iit.zdoop.Context;
import org.iit.zdoop.Mapper;

public class MyMapper extends Mapper {

	@Override
	////////////////   null,     "A B C "      "A 1 B 1  C1"
	public void map(Object key, Object value, Context context) {
		String line = String.valueOf(value);
		String[] words = line.split(" ");
		for (int i = 0; i < words.length; i++) {
			if (words[i].length() > 0) {
				context.write(words[i], 1);
			}
		}
	}

}
