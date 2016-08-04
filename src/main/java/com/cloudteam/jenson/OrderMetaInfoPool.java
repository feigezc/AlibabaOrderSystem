package com.cloudteam.jenson;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.haiwanwan.common.objectpool.ObjectFactory;
import com.haiwanwan.common.objectpool.ObjectPool;
import com.haiwanwan.common.objectpool.PoolConfig;
import com.haiwanwan.common.objectpool.Poolable;

public class OrderMetaInfoPool {
	private static ObjectPool<OrderMetaInfo> pool;
	// 大概有40万个订单block
	private static final int initSize = 80000;
	public static void init() {
		int minNum = 20;
		PoolConfig config = new PoolConfig();
		config.setPartitionSize(5);
		config.setMaxSize(initSize);
		config.setMinSize(minNum);
		config.setMaxIdleMilliseconds(60 * 1000 * 4);
		pool = new ObjectPool<OrderMetaInfo>(config, new ObjectFactory<OrderMetaInfo>() {
		       @Override
		        public OrderMetaInfo create() {
		               return new OrderMetaInfo(0, 0);
		        }
		        @Override
		        public void destroy(OrderMetaInfo o) {
		        	o = null;
		        }
		        @Override
		        public boolean validate(OrderMetaInfo o) {
		           return true;
		        }
		});		
	}
	
	public static Poolable<OrderMetaInfo> poll() {
		try {
			return pool.borrowObject(false);
		} catch (RuntimeException e) {
//			e.printStackTrace();
		}
		return null;
	}
	
	public static Poolable<OrderMetaInfo> take() {
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
	}
}