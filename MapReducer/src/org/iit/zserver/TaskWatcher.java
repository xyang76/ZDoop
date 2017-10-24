package org.iit.zserver;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.iit.zdoop.Config;
import org.iit.zdoop.JobTracker;
import org.iit.zdoop.Task;
import org.iit.zdoop.Util;

public class TaskWatcher implements Watcher {

	private ZooKeeper zk;
	private Config cfg;
	private ZMaster instance;

	public TaskWatcher(ZMaster instance) {
		this.instance = instance;
		this.zk = instance.getZk();
		this.cfg = instance.getCfg();
	}

	public void watchZNode() {
		try {
			TaskWatcher jw = new TaskWatcher(instance);
			zk.getChildren("/Tasks/Complete", jw, jw.createCallback(), null);
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
		System.out.println("Event Tasks " + event.getType() + " has been occured！");
	}

	private void doTaskComplete(String path) {
		try {
			byte[] data = zk.getData("/Tasks/Complete/" + path, true, new Stat());
			Task t = (Task) Util.deserialize(data);
			zk.delete("/Tasks/Complete/" + path, -1);
			JobTracker jt = instance.getMap().get(t.getJobid());
			if (jt.getStatus() == 1) {		// Mapping
				jt.doCollect(t);
				jt.doShuffle();
			} else {						// Reducing
				jt.doCollect(t);
				jt.doMerge();
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
						for (int i = 0; i < children.size(); i++) {
							doTaskComplete(children.get(i));
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
