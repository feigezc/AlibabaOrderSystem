package com.cloudteam.jenson;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.haiwanwan.common.objectpool.ObjectFactory;
import com.haiwanwan.common.objectpool.ObjectPool;
import com.haiwanwan.common.objectpool.PoolConfig;
import com.haiwanwan.common.objectpool.Poolable;

public class GBMetaInfoPool {
	
//	private static LinkedBlockingQueue<GBMetaInfo> pool;
	private static ObjectPool<GBMetaInfo> pool;
	private static final int initSize = 20000;
	public static void init() {
		int minNum = 20;
		PoolConfig config = new PoolConfig();
		config.setPartitionSize(5);
		config.setMaxSize(initSize);
		config.setMinSize(minNum);
		config.setMaxIdleMilliseconds(60 * 1000 * 4);
		pool = new ObjectPool<GBMetaInfo>(config, new ObjectFactory<GBMetaInfo>() {
		       @Override
		        public GBMetaInfo create() {
		               return new GBMetaInfo(0);
		        }
		        @Override
		        public void destroy(GBMetaInfo o) {
		        	o = null;
		        }
		        @Override
		        public boolean validate(GBMetaInfo o) {
		           return true;
		        }
		});
//		pool = new LinkedBlockingQueue<>(initSize);
//		for(int i = 0; i < initSize-1; i++) {
//			pool.offer(new GBMetaInfo(0));
//		}
	}
	
	public static Poolable<GBMetaInfo> poll() {
		try {
			return pool.borrowObject(false);
		} catch (RuntimeException e) {
//			e.printStackTrace();
		}
		return null;
	}
	
	public static Poolable<GBMetaInfo> take() {
		try {
			return pool.borrowObject();
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void put(Object om) {	
		if(om instanceof Poolable<?>){
			((Poolable<?>)om).returnObject();
		}
	}
	
	public static void clean() {
		try {
			pool.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
//		GBMetaInfo or = null;
//		while((or = pool.poll()) != null) {
//			or = null;
//		}
//		pool = null;
	}
}