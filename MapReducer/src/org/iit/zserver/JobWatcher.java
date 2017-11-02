package org.iit.zserver;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.iit.zdoop.Config;
import org.iit.zdoop.Job;
import org.iit.zdoop.Task;
import org.iit.zdoop.Util;
import org.iit.zserver.ZMaster.ServerWatcher;

public class JobWatcher implements Watcher {

	private ZooKeeper zk;
	private Config cfg;
	private ZMaster instance;
	private Stat stat;

	public JobWatcher(ZMaster instance) {
		this.instance = instance;
		this.zk = instance.getZk();
		this.cfg = instance.getCfg();
		this.stat = new Stat();
	}

	public void watchZNode() {
		try {
			JobWatcher jw = new JobWatcher(instance);
			zk.getChildren("/Jobs/New", jw, jw.createCallback(), null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		this.watchZNode();

		if (event.getType() == null || "".equals(event.getType())) {
			return;
		} else if (event.getType() == Event.EventType.NodeDataChanged) {
			try {
				System.out.println("data = " + new String(zk.getData(event.getPath(), true, stat)));
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println("Event Jobs " + event.getType() + " has been occured！");
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
							JobTracker tracker = new JobTracker(instance);
							tracker.doPartition(children.get(i));
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
