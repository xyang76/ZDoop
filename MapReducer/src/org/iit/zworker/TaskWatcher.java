package org.iit.zworker;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.iit.zdoop.Context;
import org.iit.zdoop.KVPair;
import org.iit.zdoop.Task;
import org.iit.zdoop.Util;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;

public class TaskWatcher implements Watcher {

	private ZooKeeper zk;
	private String name;

	public TaskWatcher(String id) {
		this.name = id;
	}

	public TaskWatcher(String id, ZooKeeper zk) {
		this.name = id;
		this.zk = zk;
	}

	@Override
	public void process(WatchedEvent event) {
		this.watchZNode();
	}

	public void watchZNode() {
		try {
			TaskWatcher tw = new TaskWatcher(name, zk);
			zk.getChildren("/Tasks/New", tw, tw.createCallback(), null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void doGetTask(String name) {
		try {
			byte[] data = zk.getData("/Tasks/New/" + name, true, new Stat());
			// TODO deserialize Task.
			Task t = (Task) Util.deserialize(data);
			zk.delete("/Tasks/New/" + name, -1);
			if (t.getStatus() == 2) {
				doReducer(t);
			} else {
				doMapper(t);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void doReducer(Task t) {
		System.out.println("Do reducer \n");
		ByteClassLoader bcl = new ByteClassLoader();
		bcl.setClassData(t.getReducerData());
		Class<?> reducer = bcl.findClass(t.getReducer());
		Context context = new Context();
		ArrayList<KVPair> part = (ArrayList<KVPair>) Util.deserialize(t.getData());

		try {
			Object instance = reducer.newInstance();
			Method[] mds = reducer.getMethods();
			for (Method m : mds) {
				if (m.getName().equals("reduce")) {
					for (int i = 0; i < part.size(); i++) {
						m.invoke(instance,
								new Object[] { part.get(i).getKey(), new Integer[] { part.get(i).getValue() }, context });
					}
					t.setData(Util.serialize(context.getResult()));
					// Mapper done, then upload it to master.
					zk.create("/Tasks/Complete/" + name, Util.serialize(t), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("Task complete\n");
				}
			}

		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void doMapper(Task t) {
		System.out.println("Do mapper \n");
		ByteClassLoader bcl = new ByteClassLoader();
		bcl.setClassData(t.getMapperData());
		Class<?> mapper = bcl.findClass(t.getMapper());
		ArrayList<String> part = (ArrayList<String>) Util.deserialize(t.getData());
		Context context = new Context();

		try {
			Object instance = mapper.newInstance();
			Method[] mds = mapper.getMethods();
			for (Method m : mds) {
				if (m.getName().equals("map")) {
					for (int i = 0; i < part.size(); i++) {
						m.invoke(instance, new Object[] { null, part.get(i), context });
					}
					t.setData(Util.serialize(context.getResult()));
					// Mapper done, then upload it to master.
					zk.create("/Tasks/Complete/" + name, Util.serialize(t), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("Task complete\n");
				}
			}

		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
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
							if (children.get(i).equals(name)) {
								System.out.println("Task detected " + name + "\n");
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
