package org.iit.zdoop;

import java.util.ArrayList;
import java.util.LinkedList;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.iit.zserver.ZMaster;

public class JobTracker {
	private ArrayList<String> mapper;
	private ArrayList<String> reducer;
	private LinkedList<Task> tasks;
	private int status;
	private int job;
	private ZooKeeper zk;
	private Config cfg;
	private ZMaster instance;
	private Stat stat;

	public JobTracker(ZMaster instance) {
		this.mapper = new ArrayList<>();
		this.reducer = new ArrayList<>();
		this.tasks = new LinkedList<>();
		this.instance = instance;
		this.zk = instance.getZk();
		this.cfg = instance.getCfg();
		this.stat = new Stat();
		this.status = 1; // Mapping
	}

	public void doCollect(Task t) {
		this.tasks.add(t);
	}

	public void doShuffle() {
		if (this.status == 1 && this.tasks.size() == this.mapper.size()) {
			this.status = 2;	// Do reducer;
			for(int i = 0; i < this.tasks.size(); i++) {
				Task t = this.tasks.remove();
				ArrayList<KVPair> data = (ArrayList<KVPair>) Util.deserialize(t.getData());
				for(int j = 0; j < data.size(); j++) {
					
				}
			}
		}
	}
	
	public int doHash(Object Key) {
		return 0;
	}

	public void doPartition(String path) {
		try {
			byte[] data = zk.getData("/Jobs/New/" + path, true, stat);
			Job job = (Job) Util.deserialize(data);
			String[] words = new String(job.getData()).split("\n");

			JobTracker tracker = this;
			int jobid = instance.getIndex();
			job.setJobid(jobid);
			tracker.setJob(jobid);

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
				t.setTaskid(instance.getIndex());
				t.setJobid(jobid);
				tracker.getMapper().add(worker);
				zk.create("/Tasks/New/" + worker, Util.serialize(t), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}

			// Set reducer
			for (int i = mapper; i < cfg.getWorkers().size(); i++) {
				tracker.getReducer().add(cfg.getWorkers().get(i));
			}

			// Delete Job
			zk.delete("/Jobs/New/" + path, -1);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void doMerge() {
		if(this.tasks.size() == this.reducer.size() && this.status == 2) {
			
		}
	}

	public LinkedList<Task> getTasks() {
		return tasks;
	}

	public void setTasks(LinkedList<Task> tasks) {
		this.tasks = tasks;
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

	public int getJob() {
		return job;
	}

	public void setJob(int job) {
		this.job = job;
	}
}
