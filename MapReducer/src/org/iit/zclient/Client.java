package org.iit.zclient;

import org.iit.zdoop.*;

public class Client {

	public static void main(String[] args) {
		// Initialize hadoop
		ZHadoop hadoop = new ZHadoop();

		// Simulate read configuration file.
		Config cfg = new Config();
		String data = "Hello world Hello world\n " + "hi world hello\n " + "nice to meet you\n" + "welcome to chicago";

		hadoop.start(cfg);

		// Start a job with mapper and reducer, also, we need specify the
		// input(data) and output(result)
		Job job = new Job();
		job.setMapper(MyMapper.class);
		job.setReducer(MyReducer.class);

		job.setData(data.getBytes());

		// Start hadoop
		hadoop.execute(job);
	}

}
