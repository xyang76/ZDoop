package org.iit.zdoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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
}
