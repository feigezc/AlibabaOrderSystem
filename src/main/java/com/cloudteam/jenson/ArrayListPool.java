package com.cloudteam.jenson;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import com.haiwanwan.common.objectpool.ObjectFactory;
import com.haiwanwan.common.objectpool.ObjectPool;
import com.haiwanwan.common.objectpool.PoolConfig;
import com.haiwanwan.common.objectpool.Poolable;

// 给MyFileWriter 同步数据时排序用
public class ArrayListPool {
	private static ObjectPool<ArrayList<FileData>> arrayListPool;
	
	public static void init() {
		int num = 500;
		int minNum = 20;
		PoolConfig config = new PoolConfig();
		config.setPartitionSize(5);
		config.setMaxSize(num);
		config.setMinSize(minNum);
		config.setMaxIdleMilliseconds(60 * 1000 * 5);
		arrayListPool = new ObjectPool<ArrayList<FileData>>(config, new ObjectFactory<ArrayList<FileData>>() {
				 @Override
				        public ArrayList<FileData> create() {
				               return new ArrayList<FileData>(4000);
				        }
				        @Override
				        public void destroy( ArrayList<FileData> o) {
				        }
				        @Override
				        public boolean validate(ArrayList<FileData> o) {
				           return true;
				        }
		});
		
	}
	
	public static Poolable<ArrayList<FileData>> take() {
		try {
			return arrayListPool.borrowObject();
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static void putBack(Poolable<ArrayList<FileData>> al) {
		al.returnObject();
	}
}