package org.iit.zworker;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZWorker {
	private ZooKeeper zk;
	private String server;
	private TaskWatcher ww;
	private String id;

	public static void main(String[] args) {
		String id = "8";
		if (args.length >= 1) {
			id = args[0];
			System.out.println(args[0]);
		} 
		new ZWorker().start(id);
	}

	public void start(String id) {
		try {
			server = "";
			this.id = id;
			ww = new TaskWatcher(id);
			zk = new ZooKeeper(server, 10000, ww);
			ww.setZk(zk);
			zk.create("/Workers/" + id, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			
//			TaskWatcher tw = new TaskWatcher(id, zk);
//			tw.watchZNode();
			while(true) {
				Thread.sleep(1000);	
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
