package com.cloudteam.jenson;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.haiwanwan.common.objectpool.ObjectFactory;
import com.haiwanwan.common.objectpool.ObjectPool;
import com.haiwanwan.common.objectpool.PoolConfig;
import com.haiwanwan.common.objectpool.Poolable;

/**
 * byte[] buffer
 * @author Jenson
 *
 */
public class ByteArrayBuffer {
	// 缓存主要给Order用。Order和Buyer的一行大都只有256B
	private static final int SIZE_128B = (1<<7) + 50;
	private static final int SIZE_300B = 300;
	private static final int SIZE_512B = (1<<9) + 50;		
	private static final int SIZE_1KB = (1<<10) + 50;	
	private static final int SIZE_2KB = (1<<11) + 50;
	private static final int SIZE_16KB = (1<<14) + 50;
	private static final int SIZE_70KB = 70 * 1024;
	
	private static ObjectPool<byte[]> size128BPool;
	private static ObjectPool<byte[]> size256BPool;
	private static ObjectPool<byte[]> size512BPool;
	private static ObjectPool<byte[]> size1KPool;
	private static ObjectPool<byte[]> size2KPool;
	private static ObjectPool<byte[]> size16KPool;
	private static ObjectPool<byte[]> size70KPool;
	
	public static boolean sizeFit(final int cap){
		boolean flag = false;
		if(cap <= SIZE_128B) {
			flag = true;
		}else if(cap <= SIZE_300B) {
			flag = true;
		}else if(cap <= SIZE_512B) {
			flag = true;
		}else if(cap <= SIZE_1KB) {
			flag = true;
		}else if(cap <= SIZE_2KB) {
			flag = true;
		}else if(cap <= SIZE_16KB) {
			flag = true;
		}else if(cap <= SIZE_70KB) {
			flag = true;
		}
		return flag;
	}
	
	public static byte[] allocate(final int cap) {
		byte[] mb = null;
//		if(cap <= SIZE_128B) {
//			mb = new byte[SIZE_128B];
//		}else if(cap <= SIZE_300B) {
//			mb = new byte[SIZE_300B];
//		}else if(cap <= SIZE_512B) {
//			mb = new byte[SIZE_512B];
//		}else if(cap <= SIZE_1KB) {
//			mb = new byte[SIZE_1KB];
//		}else if(cap <= SIZE_2KB) {
//			mb = new byte[SIZE_2KB];
//		}
		return mb;
	}
	
	public static void init() {
		// 256B,建10000个，需要2.4M内存
		int num = 5000;
		int minNum = 20;
		PoolConfig size128BConfig = new PoolConfig();
		size128BConfig.setPartitionSize(5);
		size128BConfig.setMaxSize(num);
		size128BConfig.setMinSize(minNum);
		size128BConfig.setMaxIdleMilliseconds(60 * 1000 * 4);
		size128BPool = new ObjectPool<byte[]>(size128BConfig, new ObjectFactory<byte[]>() {
		       @Override
		        public byte[] create() {
		               return new byte[SIZE_128B];
		        }
		        @Override
		        public void destroy(byte[] o) {
		        }
		        @Override
		        public boolean validate(byte[] o) {
		           return true;
		        }
		});
			
		// 300B -> 10000个，2.8m
		size256BPool = new ObjectPool<byte[]>(size128BConfig, new ObjectFactory<byte[]>() {
		       @Override
		        public byte[] create() {
		               return new byte[SIZE_300B];
		        }
		        @Override
		        public void destroy(byte[] o) {
		        }
		        @Override
		        public boolean validate(byte[] o) {
		           return true;
		        }
		});		
		// 512B -> 10000个，4M
		size512BPool = new ObjectPool<byte[]>(size128BConfig, new ObjectFactory<byte[]>() {
		       @Override
		        public byte[] create() {
		               return new byte[SIZE_512B];
		        }
		        @Override
		        public void destroy(byte[] o) {
		        }
		        @Override
		        public boolean validate(byte[] o) {
		           return true;
		        }
		});
		// 1kb -> 5000个，9m
		num = 1000;
		minNum = 3;
		PoolConfig size1KBConfig = new PoolConfig();
		size1KBConfig.setPartitionSize(5);
		size1KBConfig.setMaxSize(num);
		size1KBConfig.setMinSize(minNum);
		size1KBConfig.setMaxIdleMilliseconds(60 * 1000 * 4);
		size1KPool = new ObjectPool<byte[]>(size1KBConfig, new ObjectFactory<byte[]>() {
		       @Override
		        public byte[] create() {
		               return new byte[SIZE_1KB];
		        }
		        @Override
		        public void destroy(byte[] o) {
		        }
		        @Override
		        public boolean validate(byte[] o) {
		           return true;
		        }
		});
	
		// 2kb -> 3000个，5.8m
		size2KPool = new ObjectPool<byte[]>(size1KBConfig, new ObjectFactory<byte[]>() {
		       @Override
		        public byte[] create() {
		               return new byte[SIZE_2KB];
		        }
		        @Override
		        public void destroy(byte[] o) {
		        }
		        @Override
		        public boolean validate(byte[] o) {
		           return true;
		        }
		});
		num = 100;
		minNum = 2;
		PoolConfig size16KBConfig = new PoolConfig();
		size16KBConfig.setPartitionSize(5);
		size16KBConfig.setMaxSize(num);
		size16KBConfig.setMinSize(minNum);
		size16KBConfig.setMaxIdleMilliseconds(60 * 1000 * 3);
		// 16kb -> 512个，8m
		size16KPool = new ObjectPool<byte[]>(size16KBConfig, new ObjectFactory<byte[]>() {
		       @Override
		        public byte[] create() {
		               return new byte[SIZE_16KB];
		        }
		        @Override
		        public void destroy(byte[] o) {
		        }
		        @Override
		        public boolean validate(byte[] o) {
		           return true;
		        }
		});
		// 70kb -> 128个，8m
		size70KPool = new ObjectPool<byte[]>(size16KBConfig, new ObjectFactory<byte[]>() {
		       @Override
		        public byte[] create() {
		               return new byte[SIZE_70KB];
		        }
		        @Override
		        public void destroy(byte[] o) {
		        }
		        @Override
		        public boolean validate(byte[] o) {
		           return true;
		        }
		});

	}
	public static Poolable<byte[]> take(final int cap) {
		Poolable<byte[]> ba = null;
		if(cap <= SIZE_128B) {
			try{
				ba = size128BPool.borrowObject();
			}catch(RuntimeException ex) {
				ex.printStackTrace();
			}
			
		}else if(cap <= SIZE_300B) {
			try{
				ba = size256BPool.borrowObject();
			}catch(RuntimeException ex) {
				ex.printStackTrace();
			}
		}else if(cap <= SIZE_512B) {
			try{
				ba = size512BPool.borrowObject();
			}catch(RuntimeException ex) {
				ex.printStackTrace();
			}
		}else if(cap <= SIZE_1KB) {
			try{
				ba = size1KPool.borrowObject();
			}catch(RuntimeException ex) {
				ex.printStackTrace();
			}
		}else if(cap <= SIZE_2KB) {
			try{
				ba = size2KPool.borrowObject();
			}catch(RuntimeException ex) {
				ex.printStackTrace();
			}
			
		}else if(cap <= SIZE_16KB) {
			try{
				ba = size16KPool.borrowObject();
			}catch(RuntimeException ex) {
				ex.printStackTrace();
			}
			
		}else if(cap <= SIZE_70KB) {
			try{
				ba = size70KPool.borrowObject();
			}catch(RuntimeException ex) {
				ex.printStackTrace();
			}
			
		}
		return ba;
	}
	
	public static void putBack(final Poolable<byte[]> buf) {
//		if(buf.length == SIZE_128B) {
//			size128BPool.offer(buf);
//		}else if(buf.length == SIZE_300B) {
//			size256BPool.offer(buf);
//		}else if(buf.length == SIZE_512B) {
//			size512BPool.offer(buf);
//		}else if(buf.length == SIZE_1KB) {
//			size1KPool.offer(buf);
//		}else if(buf.length == SIZE_2KB) {
//			size2KPool.offer(buf);
//		}else if(buf.length == SIZE_16KB) {
//			size16KPool.offer(buf);
//		}else if(buf.length == SIZE_70KB) {
//			size70KPool.offer(buf);
//		}
		buf.returnObject();
		
	}
	
	
	
	public static void clean() {
//		byte[] buf = null;
//		while((buf = size128BPool.poll()) != null) {
//			buf = null;
//		}
//		while((buf = size256BPool.poll()) != null) {
//			buf = null;
//		}
//		while((buf = size512BPool.poll()) != null) {
//			buf = null;
//		}
//		while((buf = size1KPool.poll()) != null) {
//			buf = null;
//		}
//		while((buf = size2KPool.poll()) != null) {
//			buf = null;
//		}
//		while((buf = size16KPool.poll()) != null) {
//			buf = null;
//		}
//		while((buf = size70KPool.poll()) != null) {
//			buf = null;
//		}
		MyLogger.info("ByteArrayBufferPool shutdown");
		try {
			int cnt = size128BPool.shutdown();
			MyLogger.info("size128BPool shutdown done, cnt="+cnt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			int cnt = size256BPool.shutdown();
			MyLogger.info("size256BPool shutdown done, cnt="+cnt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			int cnt = size512BPool.shutdown();
			MyLogger.info("size512BPool shutdown done, cnt="+cnt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			int cnt = size1KPool.shutdown();
			MyLogger.info("size1KPool shutdown done, cnt="+cnt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			int cnt = size2KPool.shutdown();
			MyLogger.info("size2KPool shutdown done, cnt="+cnt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			int cnt = size16KPool.shutdown();
			MyLogger.info("size16KPool shutdown done, cnt="+cnt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			int cnt = size70KPool.shutdown();
			MyLogger.info("size70KPool shutdown done, cnt="+cnt);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
	}
}
