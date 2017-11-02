package org.iit.zserver;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.iit.zdoop.Config;
import org.iit.zdoop.Job;
import org.iit.zdoop.KVPair;
import org.iit.zdoop.Task;
import org.iit.zdoop.Util;

public class JobTracker {
	private ArrayList<String> mapper;
	private ArrayList<String> reducer;
	private LinkedList<Task> mapperTask;
	private LinkedList<Task> reducerTask;
	private int status;
	private Job job;
	private ZooKeeper zk;
	private Config cfg;
	private ZMaster instance;
	private Stat stat;

	public JobTracker(ZMaster instance) {
		this.mapper = new ArrayList<>();
		this.reducer = new ArrayList<>();
		this.mapperTask = new LinkedList<>();
		this.reducerTask = new LinkedList<>();
		this.instance = instance;
		this.zk = instance.getZk();
		this.cfg = instance.getCfg();
		this.stat = new Stat();
		this.status = 1; // Mapping
	}

	public void doCollect(Task t) {
		System.out.println("Master do collect task\n");
		if(t.getStatus() == 1) {
			this.mapperTask.add(t);
		} else {
			this.reducerTask.add(t);
		}
	}

	public void doShuffle() {
		
		if (this.status == 1 && this.mapperTask.size() == this.mapper.size()) {
			System.out.println("Master do shuffling\n");
			this.status = 2;	// Do reducer;
			
			ArrayList<ArrayList<KVPair>> shuffle = new ArrayList<>();
			for(int i = 0; i < this.reducer.size(); i++) {
				ArrayList<KVPair> result = new ArrayList<KVPair>();
				shuffle.add(result);
			}
			// Shuffle
			for(int i = 0; i < this.mapperTask.size(); i++) {
				Task t = this.mapperTask.remove();
				HashMap<String, Integer> data = (HashMap<String, Integer>) Util.deserialize(t.getData());
				for(Entry<String, Integer> e : data.entrySet()) {
					int index = doHash(e.getKey(), this.reducer.size());
					shuffle.get(index).add(new KVPair(e.getKey(), e.getValue()));
				}
			}
			for(int i = 0; i < this.reducer.size(); i++) {
				Task t = new Task();
				t.setData(Util.serialize(shuffle.get(i)));
				t.setJobid(job.getJobid());
				t.setReducer(job.getReducer());
				t.setReducerData(job.getReducerData());
				t.setStatus(2);
				t.setTaskid(instance.getIndex());
				// Update data;
				try {
					zk.create("/Tasks/New/" + this.reducer.get(i), Util.serialize(t), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public int doHash(String key, int reducer) {
		System.out.println("Master do hashing\n");
		int inteval = 26 / reducer;
		int value = key.toUpperCase().charAt(0) - 'A';
		if(value / inteval < reducer) {
			return value / inteval;
		} else {
			return reducer - 1;
		}
	}

	public void doPartition(String path) {
		System.out.println("Master do partition on " + path + ", we have "+ cfg.getWorkers().size() + " workers \n");
		try {
			byte[] data = zk.getData("/Jobs/New/" + path, true, stat);
			Job job = (Job) Util.deserialize(data);
			String[] words = new String(job.getData()).split("\n");
			
			// Delete Job
			zk.delete("/Jobs/New/" + path, -1);

			JobTracker tracker = this;
			int jobid = instance.getIndex();
			job.setJobid(jobid);
			job.setName(path);
			tracker.setJob(job);

			// Add tracker
			instance.getMap().put(jobid, tracker);

			int mapper = cfg.getWorkers().size() / 2;
			int reducer = cfg.getWorkers().size() - mapper;

			// Set mapper and upload data
			int k = 0;
			for (int i = 0; i < mapper; i++) {
				String worker = cfg.getWorkers().get(i);
				ArrayList<String> part = new ArrayList();
				for (int j = 0; j < words.length / mapper; j++) {
					if (k < words.length) {
						part.add(words[k]);
						k++;
					}
				}
				Task t = new Task();
				t.setData(Util.serialize(part));
				t.setMapper(job.getMapper());
				t.setReducer(job.getReducer());
				t.setMapperData(job.getMapperData());
				t.setTaskid(instance.getIndex());
				t.setJobid(jobid);
				t.setStatus(1);
				tracker.getMapper().add(worker);
				System.out.println("Do mapper on " + worker + "\n");
				if (null==zk.exists("/Tasks/New/" + worker, false)){
					zk.create("/Tasks/New/" + worker, Util.serialize(t), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} else {
					System.out.println("Node exist!");
				}
			}

			// Set reducer
			for (int i = mapper; i < cfg.getWorkers().size(); i++) {
				tracker.getReducer().add(cfg.getWorkers().get(i));
			}

			
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void doMerge() {
		ArrayList<KVPair> result = new ArrayList<>();
		if(this.reducerTask.size() == this.reducer.size() && this.status == 2) {
			System.out.println("Master do mergeing\n");
			
			for(int i = 0; i < this.mapperTask.size(); i++) {
				Task t = this.mapperTask.remove();
				HashMap<String, Integer> data = (HashMap<String, Integer>) Util.deserialize(t.getData());
				for(Entry<String, Integer> e : data.entrySet()) {
					result.add(new KVPair(e.getKey(), e.getValue()));
				}
			}
			try {
				Job job2 = new Job();
				job2.setData(Util.serialize(result));
				zk.create("/Jobs/Complete/" + job.getName(), Util.serialize(job2), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			this.status = 1;
		}
	}

	public LinkedList<Task> getTasks() {
		return mapperTask;
	}

	public void setTasks(LinkedList<Task> tasks) {
		this.mapperTask = tasks;
	}

	public ArrayList<String> getMapper() {
		return mapper;
	}

	public void setMapper(ArrayList<String> mapper) {
		this.mapper = mapper;
	}

	public ArrayList<String> getReducer() {
		return reducer;
	}

	public void setReducer(ArrayList<String> reducer) {
		this.reducer = reducer;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}
}
