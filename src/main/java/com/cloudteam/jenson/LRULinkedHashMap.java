package com.cloudteam.jenson;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRULinkedHashMap <K,V> extends LinkedHashMap<K,V>{
	private static final long serialVersionUID = -5265577819392040701L;
	private int capacity;
	public LRULinkedHashMap(final int capacity, final float loadFac, final boolean order){
		super(capacity, loadFac, order);
		//传入指定的缓存最大容量
		this.capacity = capacity;
	}
	@Override
	public boolean removeEldestEntry(Map.Entry<K, V> eldest){ 
//		System.out.println(eldest.getKey() + "=" + eldest.getValue());  
		return this.size() > this.capacity;
	}  
}
