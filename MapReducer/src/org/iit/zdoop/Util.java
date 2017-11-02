package org.iit.zdoop;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

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
    
}
