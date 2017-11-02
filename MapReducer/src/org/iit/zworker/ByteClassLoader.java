package org.iit.zworker;

public class ByteClassLoader extends ClassLoader {
	private byte[] data;

	public void setClassData(byte[] data) {
		this.data = data;
	}

	@Override
	public Class<?> findClass(String name) {
		return defineClass(name, data, 0, data.length);
	}
	
	protected Class<?> loadClass(String arg0, boolean arg1)  
            throws ClassNotFoundException {  
        Class<?> clazz = findLoadedClass(arg0);  
        if (clazz == null) {  
            if (getParent() != null) {  
                try {  
                    clazz = getParent().loadClass(arg0);  
                } catch (Exception e) {  
                    System.out.println("getParent : " + getParent());  
                    System.out.println("getParent.getparent : " + getParent().getParent());   
                    System.out.println("getParent.getparent.getparent : " + getParent().getParent().getParent());
                }  
            }  
  
            if (clazz == null) {  
                clazz = defineClass(arg0, data, 0, data.length);  
            }  
        }  
  
        return clazz;  
    }  
}