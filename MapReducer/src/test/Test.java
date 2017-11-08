package test;

import java.io.*;
import java.lang.reflect.Method;

import org.iit.zdoop.Context;
import org.iit.zdoop.Job;
import org.iit.zdoop.Mapper;
  
public class Test {  
	
	class MyMapper extends Mapper {

		@Override
		public void map(Object key, Object value, Context context) {
			String line = String.valueOf(value);
			String[] words = line.split(" ");
			for(int i = 0; i < words.length; i++) {
				if(words[i].length() > 0) {
					context.write(words[i], 1);
				}
			}
		}
	}
  
    public static void main(String[] args) {  
    	String line = "hello=ajoijf";
    	System.out.println(line.substring(line.indexOf('=') + 1));
    	
    }  
    
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
}  
