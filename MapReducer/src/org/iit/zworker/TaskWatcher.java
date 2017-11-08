package org.iit.zworker;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.iit.zdoop.Context;
import org.iit.zdoop.KVPair;
import org.iit.zdoop.Task;
import org.iit.zdoop.Util;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;

public class TaskWatcher implements Watcher {

	private ZooKeeper zk;
	private String name;
	private ZWorker worker;
	
	public TaskWatcher(ZWorker worker) {
		this.worker = worker;
		this.name = worker.getId();
		this.zk = worker.getZk();
	}

	@Override
	public void process(WatchedEvent event) {
		this.watchZNode();
	}

	public void watchZNode() {
		try {
			TaskWatcher tw = new TaskWatcher(worker);
			zk.getChildren("/Tasks/New", tw, tw.createCallback(), null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void doGetTask(String name) {
		byte[] data = Util.zooGetAndDelete(zk, "/Tasks/New/" + name);
		Task t = (Task) Util.deserialize(data);
		if (t.getStatus() == 2) {
			doReducer(t);
		} else {
			doMapper(t);
		}
	}

	private void doReducer(Task t) {
		System.out.println("Do reducer");
		ByteClassLoader bcl = new ByteClassLoader();
		bcl.setClassData(t.getReducerData());
		Class<?> reducer = bcl.findClass(t.getReducer());
		Context context = new Context();
		@SuppressWarnings("unchecked")
		ArrayList<KVPair> part = (ArrayList<KVPair>) Util.deserialize(t.getData());

		try {
			Object instance = reducer.newInstance();
			Method[] mds = reducer.getMethods();
			for (Method m : mds) {
				if (m.getName().equals("reduce")) {
					System.out.println("Task start");
					for (int i = 0; i < part.size(); i++) {
						m.invoke(instance, new Object[] { part.get(i).getKey(),
								new Integer[] { part.get(i).getValue() }, context });
					}
					t.setData(Util.serialize(context.getResult()));
					// Mapper done, then upload it to master.
					Util.zooCreate(zk, "/Tasks/Complete/" + name + "_", Util.serialize(t),
							CreateMode.PERSISTENT_SEQUENTIAL);
					if(worker.isPrint()) {
						printResult(context.getResult());
					}
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
		}
	}

	private void doMapper(Task t) {
		System.out.println("Do mapper");
		ByteClassLoader bcl = new ByteClassLoader();
		bcl.setClassData(t.getMapperData());
		Class<?> mapper = bcl.findClass(t.getMapper());
		@SuppressWarnings("unchecked")
		ArrayList<String> part = (ArrayList<String>) Util.deserialize(t.getData());
		Context context = new Context();

		try {
			Object instance = mapper.newInstance();
			Method[] mds = mapper.getMethods();
			for (Method m : mds) {
				if (m.getName().equals("map")) {
					System.out.println("Task start");
					for (int i = 0; i < part.size(); i++) {
						m.invoke(instance, new Object[] { null, part.get(i), context });
					}
					t.setData(Util.serialize(context.getResult()));
					// Mapper done, then upload it to master.
					Util.zooCreate(zk, "/Tasks/Complete/" + name + "_", Util.serialize(t),
							CreateMode.PERSISTENT_SEQUENTIAL);
					if(worker.isPrint()) {
						printResult(context.getResult());
					}
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
							if (Util.isMatch(name, children.get(i))) {
								System.out.println("Task detected " + children.get(i));
								doGetTask(children.get(i));
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
	
	public static void printResult(HashMap<String, Integer> map) {
		for(Entry<String, Integer> e : map.entrySet()) {
			System.out.println(e.getKey() + ":" + e.getValue());
		}
	}

	public ZooKeeper getZk() {
		return zk;
	}

	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}

}
