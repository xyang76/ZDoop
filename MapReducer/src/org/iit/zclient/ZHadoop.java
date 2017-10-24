package org.iit.zclient;

import java.io.*;
import java.util.List;

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
import org.iit.zdoop.Util;
import org.iit.zserver.JobWatcher;

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
		
		public void doGetJob(String path) {
			try {
				byte[] data = zk.getData(path, true, stat);
				Job j = (Job)Util.deserialize(data);
				System.out.println(new String(j.getData()));
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
	
	public void watchZNode() {
		try {
			ClientWatcher cw = new ClientWatcher();
			zk.getChildren("/Jobs/Complete", cw, cw.createCallback(), null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void start(Config config) {
		try {
			this.stat = new Stat();
			zk = new ZooKeeper(cfg.getServer(), 10000, new ClientWatcher());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void execute(Job job) {
		try {
			zk.create("/Jobs/New/J" + index, Util.serialize(job), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			index++;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}		
	}
	
}
