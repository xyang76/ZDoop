package org.iit.zworker;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

public class TaskWatcher implements Watcher {
	
	private ZooKeeper zk;
	private String name;
	
	public TaskWatcher(String id) {
		this.name = id;
	}
	
	@Override
	public void process(WatchedEvent event) {
		
	}
	
	public void watchZNode() {
		try {
			TaskWatcher tw = new TaskWatcher(name);
			zk.getChildren("/Tasks/New", tw, tw.createCallback(), null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void doGetTask(String name) {
		try {
			byte[] data = zk.getData("/Jobs/New/" + name, true, new Stat());
			// TODO deserialize Task.
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
				switch(Code.get(rc)) {
					case CONNECTIONLOSS:
						break;
					case OK:
						if(children != null) {
							for(int i = 0; i < children.size(); i++) {
								if(children.get(i).equals(name)) {
									doGetTask(name);
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
	
	public ZooKeeper getZk() {
		return zk;
	}

	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}
	
}
