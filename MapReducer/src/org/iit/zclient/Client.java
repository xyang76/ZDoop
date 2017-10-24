package org.iit.zclient;

import org.iit.zdoop.*;
import org.apache.*;

public class Client {

	public static void main(String[] args) {
		// Initialize hadoop
		ZHadoop hadoop = new ZHadoop();
		
		// Simulate read configuration file.
		Config cfg = new Config();
		String data = "Hello world Hello world\n "
				+ "hi world hello\n "
				+ "nice to meet you\n"
				+ "welcome to chicago";
		
		hadoop.start(cfg);
		
		// Start a job with mapper and reducer, also, we need specify the input(data) and output(result)
		Job job = new Job();
		job.setMapper(MyMapper.class);
		job.setReducer(MyReducer.class);
		job.setData(data.getBytes());
		
		// Start hadoop
		hadoop.execute(job);
	}
	
	class MyMapper extends Mapper {

		@Override
		public void map(Object key, Object value, Context context) {
			String line = String.valueOf(value);
			String[] words = line.split(" ");
			for(int i = 0; i < words.length; i++) {
				if(words[i].length() > 0) {
					context.write(words[i], 1);
				}
			}
		}
		
	}
	
	class MyReducer extends Reducer {

		@Override
		public void reduce(Object key, Iterable<Object> values, Context context) {
			int sum = 0;  
	        for (Object val : values) {  
	            sum += 1;	// Here we should add the sum from each partition instead of 1.
	        }  
	        context.write(String.valueOf(key), sum); 
		}
		
	}

}
