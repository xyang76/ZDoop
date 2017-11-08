package org.iit.zserver;

import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.KeeperException.Code;

public class JobWatcher implements Watcher {

	private ZooKeeper zk;
	private ZMaster instance;

	public JobWatcher(ZMaster instance) {
		this.instance = instance;
		this.zk = instance.getZk();
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

		if(instance.isDebug()) {
			System.out.println("Event Jobs " + event.getType() + " has been occured！");
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
