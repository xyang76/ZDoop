package org.iit.zserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.iit.zdoop.Config;

public class ZMaster {
	private ZooKeeper zk;
	private Config cfg;
	private ServerWatcher sw;
	private int index;
	private HashMap<Integer, JobTracker> map;
	
	class ServerWatcher implements Watcher {
		ZMaster instance;
		
		public ServerWatcher(ZMaster instance) {
			this.instance = instance;
		}
		
		@Override
		public void process(WatchedEvent event) {  
			// Register for Jobs Children change again.
//			try {
//				JobWatcher jw = new JobWatcher(instance);
//				jw.watchZNode();
//				
//				TaskWatcher tw = new TaskWatcher(instance);
//				tw.watchZNode();
//				
//				WorkerWatcher ww = new WorkerWatcher(instance);
//				ww.watchZNode();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}         
            if (event.getType() == null || "".equals(event.getType())) {  
                return;  
            } 
            EventType tp = event.getType();
            System.out.println("Event " + event.getType() + " has been occured！");  
        } 
	}

	public static void main(String[] args) {
		String id;
		if(args.length > 1) {
			id = args[1];
		} else {
			id = "1";
		}
		new ZMaster().start(id);
	}

	public void start(String id) {
		try {
			cfg = new Config();
			cfg.setServer("127.0.0.1:2181");
			zk = new ZooKeeper(cfg.getServer(), 10000, new ServerWatcher(this));
			this.index = 0;
			this.map = new HashMap<>();
			this.createNodes();
			try {
				zk.create("/Masters/m" + id, id.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} catch (NodeExistsException e) {
				
			}
			
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
		try {
			zk.create("/Tasks", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/Masters", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/Workers", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/Jobs", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/Tasks/New", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/Tasks/Complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/Jobs/New", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.create("/Jobs/Complete", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (NodeExistsException e) {
			
		}
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
}
