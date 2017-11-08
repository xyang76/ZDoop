package org.iit.zworker;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;

public class ZWorker {
	private ZooKeeper zk;
	private String server;
	private boolean debug;
	private boolean print;
	private String id;

	public static void main(String[] args) {
		new ZWorker().start(args);
	}

	public void setParameter(String[] args) {
		id = "w1";
		if (args.length >= 1) {
			id = args[0];
			System.out.println(args[0]);
			if(args.length >= 2) {
				for(int i = 1; i < args.length; i++) {
					if("-debug".equals(args[i])) {
						debug = true;
					}
					if("-print".equals(args[i])) {
						print = true;
					}
				}
			}
		} 
	}

	public void start(String[] args) {
		try {
			setParameter(args);
			server = "127.0.0.1:2181";
			zk = new ZooKeeper(server, 10000, new Watcher(){
				@Override
				public void process(WatchedEvent event) {
					
				}
			});
			zk.create("/Workers/" + id, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			
			TaskWatcher ww = new TaskWatcher(this);
			ww.setZk(zk);
			ww.watchZNode();
			
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
	
	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}
	
	public boolean isPrint() {
		return print;
	}

	public void setPrint(boolean print) {
		this.print = print;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public ZooKeeper getZk() {
		return zk;
	}

	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}
}
