package org.iit.zclient;

import org.iit.zdoop.*;

public class Client {

	public static void main(String[] args) {
		// Initialize hadoop
		ZHadoop hadoop = new ZHadoop();

		// Simulate read configuration file.
		Config cfg = new Config();
		String data = "Hello world hello world\n " + "hi world hello you\n " + "nice to meet you\n"
				+ "Hello welcome to chicago";

		hadoop.start(cfg, args);

		// Start a job with mapper and reducer, also, we need specify the
		// input(data) and output(result)
		if(!hadoop.isCmdMode()) {
			Job job = new Job();
			job.setMapper(MyMapper.class);
			job.setReducer(MyReducer.class);
	
			job.setData(data.getBytes());
	
			// Start hadoop
			hadoop.execute(job);
		} else {
			hadoop.startcmd();
		}
	}

}
