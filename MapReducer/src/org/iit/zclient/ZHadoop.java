package org.iit.zclient;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.iit.zdoop.Config;
import org.iit.zdoop.Job;
import org.iit.zdoop.KVPair;
import org.iit.zdoop.Util;

public class ZHadoop {
	private Config cfg; 
	private ZooKeeper zk;
	private int index = 0;
	private Stat stat;
	
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
			try {
				byte[] data = zk.getData(path, true, stat);
				zk.delete("/Jobs/Complete/" + path, -1);
				Job j = (Job)Util.deserialize(data);
				ArrayList<KVPair> result = (ArrayList<KVPair>) Util.deserialize(j.getData());
				for(KVPair e : result) {
					System.out.println(e.getKey() + " " + e.getValue() + "\n");
				}
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
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
								System.out.println("A new job complete at /Jobs/Complete/" +  children.get(i) + "\n");
								doGetJob("/Jobs/Complete/" + children.get(i));
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
	
	public void start(Config config) {
		try {
			this.stat = new Stat();
			zk = new ZooKeeper("127.0.0.1:2181", 10000, new ClientWatcher());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void execute(Job job) {
		try {
			job.setMapperData(readClass(job.getMapper()));
			job.setReducerData(readClass(job.getReducer()));
			zk.create("/Jobs/New/J" + index, Util.serialize(job), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			System.out.println("A new job: J" + index  + " has been submitted\n");
			index++;
			
			while(true) {
				Thread.sleep(1000);	
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
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
	
}
