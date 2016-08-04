package com.cloudteam.jenson;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.haiwanwan.common.objectpool.ObjectFactory;
import com.haiwanwan.common.objectpool.ObjectPool;
import com.haiwanwan.common.objectpool.PoolConfig;
import com.haiwanwan.common.objectpool.Poolable;

import sun.misc.Cleaner;

public class ByteBufferPool {
	
	// 一个订单索引大概占17byte,一个商品、买家索引占31byte,平均一个占24byte
	private static final int SIZE_8NUM = 40;		// 40B
	private static final int SIZE_16NUM = 512;		// 512B
	private static final int SIZE_32NUM = 1024;		// 1KB
	private static final int SIZE_64NUM = 2048;		// 1984B
	private static final int SIZE_100NUM = 2500;	// 2.3K
	
//	private static final int SIZE_256B = (1<<8);
//	private static final int SIZE_300B = 300;
//	private static final int SIZE_512B = (1<<9);		
//	private static final int SIZE_1024B = (1<<10) + 50;
//	private static final int SIZE_2048B = (1<<11) + 50;
//	
//	private static ConcurrentLinkedQueue<ByteBuffer> size256BPool = new ConcurrentLinkedQueue<>();
	private static ObjectPool<ByteBuffer> 	size8NumPool;
	private static ObjectPool<ByteBuffer> size16NumPool;
	private static ObjectPool<ByteBuffer> size32NumPool;
	private static ObjectPool<ByteBuffer> size64NumPool;
	private static ObjectPool<ByteBuffer> size100NumPool;
	
	/**
	 * 判断cap是否在Pool的范围内
	 * @param cap
	 * @return
	 */
	public static boolean sizeFix(final int cap) {
		boolean flag = false;
		if(cap <= SIZE_8NUM) {
			flag = true;
		}else if(cap <= SIZE_16NUM) {
			flag = true;;
		}else if(cap <= SIZE_32NUM) {
			flag = true;
		}else if(cap <= SIZE_64NUM) {
			flag = true;
		}else if(cap <= SIZE_100NUM) {
			flag = true;
		}
		return flag;
	}
	
	public static void init() {
		// 256B,最多15000个，需要2.4M内存
		int num = 10000;
		int minNum = 50;
		PoolConfig size8NumConfig = new PoolConfig();
		size8NumConfig.setPartitionSize(5);
		size8NumConfig.setMaxSize(num);
		size8NumConfig.setMinSize(minNum);
		size8NumConfig.setMaxIdleMilliseconds(60 * 1000 * 5);
		size8NumPool = new ObjectPool<ByteBuffer>(size8NumConfig, new ObjectFactory<ByteBuffer>() {
            @Override
            public ByteBuffer create() {
                return ByteBuffer.allocateDirect(SIZE_8NUM);
            }
            @Override
            public void destroy(ByteBuffer directBuf) {
            	if(directBuf.isDirect()){
	            	try {
						Field cleanerField = directBuf.getClass().getDeclaredField("cleaner");
						cleanerField.setAccessible(true);
						Cleaner cleaner = (Cleaner) cleanerField.get(directBuf);
						cleaner.clean();
					} catch (Exception e) {
						e.printStackTrace();
					}
            	}
            }
            @Override
            public boolean validate(ByteBuffer o) {
                return true;
            }
        });
		size16NumPool = new ObjectPool<ByteBuffer>(size8NumConfig, new ObjectFactory<ByteBuffer>() {
            @Override
            public ByteBuffer create() {
                return ByteBuffer.allocate(SIZE_16NUM);
            }
            @Override
            public void destroy(ByteBuffer o) {
            }
            @Override
            public boolean validate(ByteBuffer o) {
                return true;
            }
        });
		size32NumPool = new ObjectPool<ByteBuffer>(size8NumConfig, new ObjectFactory<ByteBuffer>() {
            @Override
            public ByteBuffer create() {
                return ByteBuffer.allocate(SIZE_32NUM);
            }
            @Override
            public void destroy(ByteBuffer o) {
            }
            @Override
            public boolean validate(ByteBuffer o) {
                return true;
            }
        });
		size64NumPool = new ObjectPool<ByteBuffer>(size8NumConfig, new ObjectFactory<ByteBuffer>() {
            @Override
            public ByteBuffer create() {
                return ByteBuffer.allocate(SIZE_64NUM);
            }
            @Override
            public void destroy(ByteBuffer o) {
            }
            @Override
            public boolean validate(ByteBuffer o) {
                return true;
            }
        });
		
		// 2.3K,建1000个，需要2.3m内存
		num = 200;
		minNum = 2;
		PoolConfig size100NumConfig = new PoolConfig();
		size100NumConfig.setPartitionSize(5);
		size100NumConfig.setMaxSize(num);
		size100NumConfig.setMinSize(minNum);
		size100NumConfig.setMaxIdleMilliseconds(60 * 1000 * 5);
		size100NumPool = new ObjectPool<ByteBuffer>(size8NumConfig, new ObjectFactory<ByteBuffer>() {
            @Override
            public ByteBuffer create() {
                return ByteBuffer.allocate(SIZE_100NUM);
            }
            @Override
            public void destroy(ByteBuffer o) {
            }
            @Override
            public boolean validate(ByteBuffer o) {
                return true;
            }
        });
		
		// 总共38M内存
	}
	
	public static Poolable<ByteBuffer> poll(final int cap){
		Poolable<ByteBuffer> buf = null;
		if(cap <= SIZE_8NUM) {
			try{
				buf = size8NumPool.borrowObject(false);
			}catch(RuntimeException re) {
//				re.printStackTrace();
			}
		}else if(cap <= SIZE_16NUM) {
			try{
				buf = size16NumPool.borrowObject(false);
			}catch(RuntimeException re) {
//				re.printStackTrace();
			}
		}else if(cap <= SIZE_32NUM) {
			try{
				buf = size32NumPool.borrowObject(false);
			}catch(RuntimeException re) {
//				re.printStackTrace();
			}
		}else if(cap <= SIZE_64NUM) {
			try{
				buf = size64NumPool.borrowObject(false);
			}catch(RuntimeException re) {
//				re.printStackTrace();
			}
		}else if(cap <= SIZE_100NUM) {
			try{
				buf = size100NumPool.borrowObject(false);
			}catch(RuntimeException re) {
//				re.printStackTrace();
			}
		}
		return buf;
	}
	public static Poolable<ByteBuffer> take(final int cap) {
		Poolable<ByteBuffer> buf = null;
		if(cap <= SIZE_8NUM) {
			try{
				buf = size8NumPool.borrowObject(true);
			}catch(RuntimeException re) {
				re.printStackTrace();
			}
		}else if(cap <= SIZE_16NUM) {
			try{
				buf = size16NumPool.borrowObject(true);
			}catch(RuntimeException re) {
				re.printStackTrace();
			}
		}else if(cap <= SIZE_32NUM) {
			try{
				buf = size32NumPool.borrowObject(true);
			}catch(RuntimeException re) {
				re.printStackTrace();
			}
		}else if(cap <= SIZE_64NUM) {
			try{
				buf = size64NumPool.borrowObject(true);
			}catch(RuntimeException re) {
				re.printStackTrace();
			}
		}else if(cap <= SIZE_100NUM) {
			try{
				buf = size100NumPool.borrowObject(true);
			}catch(RuntimeException re) {
				re.printStackTrace();
			}
		}
		
		return buf;
	}
	
	public static ByteBuffer allocate(final int cap){
//		ByteBuffer buf = null;
//		if(cap <= SIZE_8NUM) {
//			buf = ByteBuffer.allocate(SIZE_8NUM);
//		}else if(cap <= SIZE_16NUM) {
//			buf = ByteBuffer.allocate(SIZE_16NUM);
//		}else if(cap <= SIZE_32NUM) {
//			buf = ByteBuffer.allocate(SIZE_32NUM);
//		}else if(cap <= SIZE_64NUM) {
//			buf = ByteBuffer.allocate(SIZE_64NUM);
//		}else if(cap <= SIZE_100NUM) {
//			buf = ByteBuffer.allocate(SIZE_100NUM);
//		}
		return null;
	}
	
	public static void putBack(final Poolable<ByteBuffer> pool) {
//		if(directBuf.capacity() == SIZE_8NUM) {
//			size8NumPool.offer(directBuf);
//		}else if(directBuf.capacity() == SIZE_16NUM) {
//			size16NumPool.offer(directBuf);
//		}else if(directBuf.capacity() == SIZE_32NUM) {
//			size32NumPool.offer(directBuf);
//		}else if(directBuf.capacity() == SIZE_64NUM) {
//			size64NumPool.offer(directBuf);
//		}else if(directBuf.capacity() == SIZE_100NUM) {
//			size100NumPool.offer(directBuf);
//		}
		pool.returnObject();
	}
	
	
	
	public static void clean() {
		try {
			size8NumPool.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			size16NumPool.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			size32NumPool.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			size64NumPool.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			size100NumPool.shutdown();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		MyLogger.info("ByteBufferPool shutdown done");
//		ByteBuffer buf = null;
//		while((buf = size8NumPool.poll()) != null) {
//			buf.clear();
//			buf = null;
//		}
//		while((buf = size16NumPool.poll()) != null) {
//			buf.clear();
//			buf = null;
//		}
//		while((buf = size32NumPool.poll()) != null) {
//			buf.clear();
//			buf = null;
//		}
//		while((buf = size64NumPool.poll()) != null) {
//			buf.clear();
//			buf = null;
//		}
//		while((buf = size100NumPool.poll()) != null) {
//			buf.clear();
//			buf = null;
//		}
	}
}