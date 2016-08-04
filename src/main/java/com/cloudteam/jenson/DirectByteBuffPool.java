package com.cloudteam.jenson;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;
import sun.misc.Cleaner;

/**
 * 试试将DirectByteBuffer 放入缓存池来使用，以避免不停的new
 * @author Jenson
 *
 */
public class DirectByteBuffPool {
	// 供查询时使用;主要由16KB Good, 32KB Order和Buyer
	private static final int SIZE_16KB = (1<<14);		// 16kB
	private static final int SIZE_32KB = (1<<15);		// 32kB
	private static final int SIZE_64KB = (1<<16);		// 64kB
	
	private static ConcurrentLinkedQueue<ByteBuffer> size64KBPool = new ConcurrentLinkedQueue<>();
	private static ConcurrentLinkedQueue<ByteBuffer> size16KBPool = new ConcurrentLinkedQueue<>();
	private static ConcurrentLinkedQueue<ByteBuffer> size32KBPool = new ConcurrentLinkedQueue<>();
	
	// 一个订单索引大概占17byte,一个商品、买家索引占31byte,平均一个占24byte
	private static final int SIZE_8NUM = 256;		// 256B
	private static final int SIZE_16NUM = 512;		// 512B
	private static final int SIZE_32NUM = 1024;		// 1KB
	private static final int SIZE_64NUM = 2048;		// 2KB
	private static final int SIZE_100NUM = 2500;	// 2.4K
	
//	private static final int SIZE_300B = 300;	
//	private static final int SIZE_512B = (1<<9);		
//	private static final int SIZE_1024B = (1<<10) + 50;
//	private static final int SIZE_2048B = (1<<11) + 50;
	
	private static ConcurrentLinkedQueue<ByteBuffer> size8NumPool = new ConcurrentLinkedQueue<>();
	private static ConcurrentLinkedQueue<ByteBuffer> size16NumPool = new ConcurrentLinkedQueue<>();
	private static ConcurrentLinkedQueue<ByteBuffer> size32NumPool = new ConcurrentLinkedQueue<>();
	private static ConcurrentLinkedQueue<ByteBuffer> size64NumPool = new ConcurrentLinkedQueue<>();
	private static ConcurrentLinkedQueue<ByteBuffer> size100NumPool = new ConcurrentLinkedQueue<>();
	/**
	 * 初始化B大小的pool，为了方便写文件
	 */
	public static void initSmallPool() {
//		// 256B,建10000个，需要2.4M内存
//		int num = 10000;
//		for(int i = 0; i < num; i++) {
//			ByteBuffer directBuf = ByteBuffer.allocateDirect(SIZE_8NUM);
//			size8NumPool.offer(directBuf);
//		}
//		// 512B,建10000个，需要3.6m内存
//		num = 10000;
//		for(int i = 0; i < num; i++) {
//			ByteBuffer directBuf = ByteBuffer.allocateDirect(SIZE_16NUM);
//			size16NumPool.offer(directBuf);
//		}
//		// 1024B,建40000个，需要38.8m内存
//		num = 40000;
//		for(int i = 0; i < num; i++) {
//			ByteBuffer directBuf = ByteBuffer.allocateDirect(SIZE_32NUM);
//			size32NumPool.offer(directBuf);
//		}
//		// 2K,建5000个，需要9m内存
//		num = 5000;
//		for(int i = 0; i < num; i++) {
//			ByteBuffer directBuf = ByteBuffer.allocateDirect(SIZE_64NUM);
//			size64NumPool.offer(directBuf);
//		}
//		// 2.5K,建1000个，需要2.4m内存
//		num = 1000;
//		for(int i = 0; i < num; i++) {
//			ByteBuffer directBuf = ByteBuffer.allocateDirect(SIZE_100NUM);
//			size100NumPool.offer(directBuf);
//		}
//		// 总共55M堆外内存
	}
	public static ByteBuffer takeSmallPool(final int cap) {
		ByteBuffer buf = null;		
		if(cap <= SIZE_8NUM) {
			buf = size8NumPool.poll();
			if(buf == null) {
				buf = size16NumPool.poll();
			}
			if(buf == null) {
				buf = size32NumPool.poll();
			}
		}else if(cap <= SIZE_16NUM) {
			buf = size16NumPool.poll();
			if(buf == null) {
				buf = size32NumPool.poll();
			}
			if(buf == null) {
				buf = size64NumPool.poll();
			}
		}else if(cap <= SIZE_32NUM) {
			buf = size32NumPool.poll();
			if(buf == null) {
				buf = size64NumPool.poll();
			}
			if(buf == null) {
				buf = size100NumPool.poll();
			}
		}else if(cap <= SIZE_64NUM) {
			buf = size64NumPool.poll();
			if(buf == null) {
				buf = size100NumPool.poll();
			}
		}else if(cap <= SIZE_100NUM) {
			buf = size100NumPool.poll();
		}
		return buf;
	}
	
	public static void putBackSmallPool(final ByteBuffer buf,final int cap) {
//		MyLogger.info("directBuffer put back:" + buf.capacity());
		if(buf.capacity() == SIZE_8NUM) {
			size8NumPool.offer(buf);
		}else if(buf.capacity() == SIZE_16NUM) {
			size16NumPool.offer(buf);
		}else if(buf.capacity() == SIZE_32NUM) {
			size32NumPool.offer(buf);
		}else if(buf.capacity() == SIZE_64NUM) {
			size32NumPool.offer(buf);
		}else if(buf.capacity() == SIZE_100NUM) {
			size32NumPool.offer(buf);
		}
	}
	
	public static void cleanSmallPool() {
		 ByteBuffer directBuf = null;
		 while((directBuf = size8NumPool.poll()) != null) {
			try {
				Field cleanerField = directBuf.getClass().getDeclaredField("cleaner");
				cleanerField.setAccessible(true);
				Cleaner cleaner = (Cleaner) cleanerField.get(directBuf);
				cleaner.clean();
			} catch (Exception e) {
				e.printStackTrace();
			}
		 }
		 while((directBuf = size16NumPool.poll()) != null) {
			try {
					Field cleanerField = directBuf.getClass().getDeclaredField("cleaner");
					cleanerField.setAccessible(true);
					Cleaner cleaner = (Cleaner) cleanerField.get(directBuf);
					cleaner.clean();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
		 while((directBuf = size32NumPool.poll()) != null) {
				try {
					Field cleanerField = directBuf.getClass().getDeclaredField("cleaner");
					cleanerField.setAccessible(true);
					Cleaner cleaner = (Cleaner) cleanerField.get(directBuf);
					cleaner.clean();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
		 while((directBuf = size64NumPool.poll()) != null) {
				try {
					Field cleanerField = directBuf.getClass().getDeclaredField("cleaner");
					cleanerField.setAccessible(true);
					Cleaner cleaner = (Cleaner) cleanerField.get(directBuf);
					cleaner.clean();
				} catch (Exception e) {
					e.printStackTrace();
				}
		}
		 while((directBuf = size100NumPool.poll()) != null) {
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
	
	/**
	 * 初始化KB大小的pool，为了方便读文件
	 */
	public static void initBigPool() {
		// 32KB需求最多,建64个，需要2M内存
		for(int i = 0; i < 64; i++) {
			ByteBuffer directBuf = ByteBuffer.allocateDirect(SIZE_64KB);
			size64KBPool.offer(directBuf);
		}
		// 16KB都建16个,分别需要:256KB
		for(int i = 0; i < 16; i++) {
			ByteBuffer directBuf = ByteBuffer.allocateDirect(SIZE_16KB);
			size16KBPool.offer(directBuf);
		}
		// 64KB 罕见
		for(int i = 0; i < 4; i++) {
			ByteBuffer directBuf = ByteBuffer.allocateDirect(SIZE_64KB);
			size64KBPool.offer(directBuf);
		}
	}
	
	
	/**
	 * 大小必须至少8KB
	 * @param cap
	 * @return
	 */
	public static ByteBuffer takeBigPool(final int cap) {
		if(cap <= SIZE_16KB) {
			return size16KBPool.poll();
		}else if(cap <= SIZE_32KB) {
			return size32KBPool.poll();
		}else if(cap <= SIZE_64KB) {
			return size64KBPool.poll();
		}
		return null;
	}
	
	public static void putBackBigPool(final ByteBuffer buf, final int cap) {
		if(buf.capacity() == SIZE_64KB) {
			buf.clear();
			putback64KBuf(buf);
		}else if(buf.capacity() == SIZE_16KB) {
			buf.clear();
			size16KBPool.offer(buf);
		}else if(buf.capacity() == SIZE_32KB) {
			buf.clear();
			size32KBPool.offer(buf);
		}
	}
	
	private static ByteBuffer take64KBuf() {
		return size64KBPool.poll();
	}
	
	private static void putback64KBuf(final ByteBuffer buf) {
		 size64KBPool.offer(buf);
	}
	
}
