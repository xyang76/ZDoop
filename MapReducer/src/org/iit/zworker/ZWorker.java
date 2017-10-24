package org.iit.zworker;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.iit.zdoop.Config;

public class ZWorker {
	private ZooKeeper zk;
	private String server;
	private TaskWatcher ww;
	private String id;

	public static void main(String[] args) {
		String id = args[1];
		if(args.length > 1) {
			id = args[1];
		} else {
			id = "1";
		}
		new ZWorker().start(id);
	}

	public void start(String id) {
		try {
			server = "";
			this.id = id;
			ww = new TaskWatcher(id);
			ww.setZk(zk);
			
			zk = new ZooKeeper(server, 10000, ww);
			zk.create("/Workers/w" + id, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
