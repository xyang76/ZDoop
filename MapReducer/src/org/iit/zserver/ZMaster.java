package org.iit.zserver;

import java.io.IOException;
import java.util.HashMap;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.iit.zdoop.Config;
import org.iit.zdoop.Util;

public class ZMaster {
	private ZooKeeper zk;
	private Config cfg;
	private int index;
	private boolean debug;
	private boolean print;
	private String id;
	private HashMap<Integer, JobTracker> map;
	
	class ServerWatcher implements Watcher {
		ZMaster instance;
		
		public ServerWatcher(ZMaster instance) {
			this.instance = instance;
		}
		
		@Override
		public void process(WatchedEvent event) {  
			if(instance.isDebug()) {
				System.out.println("Event " + event.getType() + " has been occured！");  
			}
        } 
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
						debug = true;
					}
				}
			}
		} 
	}

	public static void main(String[] args) {
		new ZMaster().start(args);
	}

	public void start(String[] args) {
		try {
			setParameter(args);
			cfg = new Config();
			cfg.setServer("127.0.0.1:2181");
			zk = new ZooKeeper(cfg.getServer(), 10000, new ServerWatcher(this));
			this.index = 0;
			this.map = new HashMap<>();
			this.createNodes();
			Util.zooCreate(zk, "/Masters/m" + id, id.getBytes(), CreateMode.EPHEMERAL);
			
			JobWatcher jw = new JobWatcher(this);
			jw.watchZNode();
			
			TaskWatcher tw = new TaskWatcher(this);
			tw.watchZNode();
			
			WorkerWatcher ww = new WorkerWatcher(this);
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

	public void createNodes() throws KeeperException, InterruptedException {
		Util.zooCreate(zk, "/Tasks", null, CreateMode.PERSISTENT, false);
		Util.zooCreate(zk, "/Masters", null, CreateMode.PERSISTENT, false);
		Util.zooCreate(zk, "/Workers", null, CreateMode.PERSISTENT, false);
		Util.zooCreate(zk, "/Jobs", null, CreateMode.PERSISTENT, false);
		Util.zooCreate(zk, "/Tasks/New", null, CreateMode.PERSISTENT, false);
		Util.zooCreate(zk, "/Tasks/Complete", null, CreateMode.PERSISTENT, false);
		Util.zooCreate(zk, "/Jobs/New", null, CreateMode.PERSISTENT, false);
		Util.zooCreate(zk, "/Jobs/Complete", null, CreateMode.PERSISTENT, false);
	}
	
	public ZooKeeper getZk() {
		return zk;
	}

	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}
	
	public HashMap<Integer, JobTracker> getMap() {
		return map;
	}

	public void setMap(HashMap<Integer, JobTracker> map) {
		this.map = map;
	}

	public Config getCfg() {
		return cfg;
	}

	public void setCfg(Config cfg) {
		this.cfg = cfg;
	}

	public int getIndex() {
		return index++;
	}

	public void setIndex(int index) {
		this.index = index;
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
}
