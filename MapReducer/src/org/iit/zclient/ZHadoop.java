package org.iit.zclient;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.iit.zdoop.Config;
import org.iit.zdoop.Job;
import org.iit.zdoop.KVPair;
import org.iit.zdoop.Util;

public class ZHadoop {
	private ZooKeeper zk;
	private String id;
	private Scanner sc;
	private boolean cmdMode;
	private boolean hasResult;

	class ClientWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			watchZNode();
		}
		
		public void watchZNode() {
			try {
				ClientWatcher cw = new ClientWatcher();
				zk.getChildren("/Jobs/Complete", cw, cw.createCallback(), null);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public void doGetJob(String path) {
			byte[] data = Util.zooGetData(zk, path);
			Util.zooDelete(zk, path);
			Job j = (Job)Util.deserialize(data);
			@SuppressWarnings("unchecked")
			ArrayList<KVPair> result = (ArrayList<KVPair>) Util.deserialize(j.getData());
			System.out.println("Result:");
			for(KVPair e : result) {
				System.out.println(e.getKey() + " " + e.getValue());
			}
			hasResult = true;
		}
		
		public ChildrenCallback createCallback() {
			return new ChildrenCallback() {

				@Override
				public void processResult(int rc, String path, Object ctx, List<String> children) {
					switch (Code.get(rc)) {
					case CONNECTIONLOSS:
						break;
					case OK:
						if (children != null) {
							for(int i = 0; i < children.size(); i++) {
								if(Util.isMatch(id, children.get(i))) {
									doGetJob("/Jobs/Complete/" + children.get(i));
								}
							}
						}
						break;
					default:
						break;
					}
				}
			};
		}
	}
	
	public void start(Config config, String[] args) {
		try {
			id = "c1";
			cmdMode = false;
			if (args.length >= 1) {
				id = args[0];
				if(args.length >= 2) {
					for(int i = 1; i < args.length; i++) {
						if("-cmd".equals(args[i])) {
							cmdMode = true;
						}
					}
				}
			} 
			zk = new ZooKeeper("127.0.0.1:2181", 10000, new ClientWatcher());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void execute(Job job) {
		job.setMapperData(readClass(job.getMapper()));
		job.setReducerData(readClass(job.getReducer()));
		Util.zooCreate(zk, "/Jobs/New/" + id + "_", Util.serialize(job), CreateMode.PERSISTENT_SEQUENTIAL);
		System.out.println("A new job has been submitted\n");
		
		if(!cmdMode) {
			while(true) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}	
			}
		}
	}
	
	public byte[] readClass(String classT) {
		String c = System.getProperty("user.dir") + "/";
		String path = classT.replace(".", "/");
		String[] paths = new String[3];
		paths[0] = c + "bin/" + path + ".class";
		paths[1] = c + path + ".class";
		paths[2] = c + path.substring(path.lastIndexOf("/") + 1) + ".class";
		
		for(int i = 0; i < paths.length; i++) {
			File f = new File(paths[i]);
			if(f.exists()) {
				return Util.readFile(paths[i]);
			}
		}
		System.out.println("No class exist!");
		return null;
	}
	
	public boolean isCmdMode() {
		return cmdMode;
	}

	public void setCmdMode(boolean cmdMode) {
		this.cmdMode = cmdMode;
	}

	public void startcmd() {
		sc = new Scanner(System.in);
		Job job = new Job();
		boolean mapper = false, reducer = false, data = false;
		hasResult = false;
		System.out.println("Please input a command:");
		while(true) {
			String line = sc.nextLine();
			if(Util.isMatch("submit", line.toLowerCase())) {
				if(mapper && reducer && data) {
					this.execute(job);
					job = new Job();
					break;
				} else {
					System.out.println("Please submit mapper, reducer and data!");
					continue;
				}
			} 
			String path = line.substring(line.indexOf('=') + 1).trim();
			String[] args = path.split(" ");
			File f = new File(args[0]);
			if(!f.exists()) {
				System.out.println("File not exist!");
				continue;
			}
			if(Util.isMatch("mapper", line.toLowerCase())) {
				mapper = true;
				job.setMapperData(Util.readFile(args[0]));
				job.setMapper(args[1]);
				System.out.println("Set mapper success!");
			} else if(Util.isMatch("reducer", line.toLowerCase())) {
				reducer = true;
				job.setReducerData(Util.readFile(args[0]));
				job.setReducer(args[1]);
				System.out.println("Set reducer success!");
			} else if(Util.isMatch("data", line.toLowerCase())) {
				data = true;
				job.setData(Util.readFile(path));
				System.out.println("Set data success!");
			}
		}
		while(!hasResult) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}	
		}
	}
}
