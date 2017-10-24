package org.iit.zserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.iit.zdoop.Config;
import org.iit.zserver.ZMaster.ServerWatcher;

public class WorkerWatcher implements Watcher {
	
	private ZooKeeper zk;
	private Config cfg;
	private ZMaster instance;
	
	public WorkerWatcher(ZMaster instance){
		this.instance = instance;
		this.zk = instance.getZk();
		this.cfg = instance.getCfg();
	}
	
	public void watchZNode(){
		try {
			WorkerWatcher jw = new WorkerWatcher(instance);
			zk.getChildren("/Workers", jw, jw.createCallback(), null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		this.watchZNode();
		
        if (event.getType() == null || "".equals(event.getType())) {  
            return;  
        } 
        System.out.println("Event Workers " + event.getType() + " has been occured！");  
	}
	
	private void doSetWorker(List<String> children) {
		ArrayList<String> workers = new ArrayList<String>();
		for(int i = 0; i < children.size(); i++) {
			workers.add(children.get(i));
		}
		cfg.setWorkers(workers);
	}
	
	public ChildrenCallback createCallback() {
		return new ChildrenCallback() {

			@Override
			public void processResult(int rc, String path, Object ctx, List<String> children) {
				switch(Code.get(rc)) {
					case CONNECTIONLOSS:
						break;
					case OK:
						if(children != null) {
							doSetWorker(children);
						}
						break;
					default:
						break;
				}
			}
		};
	}
	
	public void doPartition() {
		
	}
	
	public ZooKeeper getZk() {
		return zk;
	}

	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}
}
