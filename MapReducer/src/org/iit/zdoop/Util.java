package org.iit.zdoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class Util {
	public static byte[] serialize(Object o) {
		ByteArrayOutputStream op = new ByteArrayOutputStream();
	    ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(op);
			oos.writeObject(o);
			oos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				oos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return op.toByteArray();
	}
    
    public static Object deserialize(byte[] data) {
    	ByteArrayInputStream bi = new ByteArrayInputStream(data);
    	ObjectInputStream ois = null;
    	Object rv = null;
    	try {
    		ois = new ObjectInputStream(bi);
    		rv = ois.readObject();
    		ois.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rv;
    }
    
    public static byte[] readFile(String path) {
        byte[] buffer = null;  
        try {  
            File file = new File(path);  
            FileInputStream fis = new FileInputStream(file);  
            ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);  
            byte[] b = new byte[(int) file.length()];  
            int n;  
            while ((n = fis.read(b)) != -1) {  
                bos.write(b, 0, n);  
            }  
            fis.close();  
            bos.close();  
            buffer = bos.toByteArray();  
        } catch (FileNotFoundException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
        return buffer;  
    }
    
    public static void zooDelete(ZooKeeper zk, String path) {
    	try {
    		zk.delete(path, -1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
    }
    
    public static void zooCreate(ZooKeeper zk, String path, byte[] data, CreateMode mode) {
    	try {
			zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    public static void zooCreate(ZooKeeper zk, String path, byte[] data, CreateMode mode, boolean isNotify) {
    	try {
			zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
		} catch (KeeperException e) {
			if(isNotify) {
				e.printStackTrace();
			}
		} catch (InterruptedException e) {
			if(isNotify) {
				e.printStackTrace();
			}
		}
    }
    
    public static byte[] zooGetData(ZooKeeper zk, String path) {
    	Stat stat = new Stat();
    	try {
			return zk.getData(path, true, stat);
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	return new byte[0];
    }
    
    public static byte[] zooGetAndDelete(ZooKeeper zk, String path) {
    	Stat stat = new Stat();
    	byte[] data;
    	try {
			data = zk.getData(path, true, stat);
			zk.delete(path, -1);
			return data;
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    	return new byte[0];
    }
    
    public static boolean isMatch(String id, String path) {
		boolean flag = true;
		for(int i = 0; i < id.length(); i++) {
			if(id.charAt(i) != path.charAt(i)) {
				flag = false;
				break;
			}
		}
		return flag;
	}
    
    
}
