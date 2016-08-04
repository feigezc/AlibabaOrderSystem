package com.cloudteam.jenson;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.haiwanwan.common.objectpool.Poolable;

/**
 * 每个文件分别缓存自己的数据，然后攒到一定量以后就批量写
 * @author Jenson
 *
 */
class FileData implements Comparable<FileData> {
	public long position;
	public Poolable<ByteBuffer> data;
	public int dataSize;
	public boolean isPool;
	public FileData(final long pos, final Poolable<ByteBuffer> dt,
			final int dataSize, final boolean flag) {
		this.position = pos;
		this.data = dt;
		this.dataSize = dataSize;
		this.isPool = flag;
	}
	@Override
	public int compareTo(FileData o) {
		// 升序
		if(this.position < o.position) {
			return -1;
		}else if(this.position > o.position) {
			return 1;
		}
		return 0;
	}
}
public final class MyFileWriter {
	private static final int SIZE_8MB = (1<<23);		
	private static final int SIZE_16MB = (1<<24);
	private static final int SIZE_32MB = (1<<25);
	
	private String filePath;
	public String getFilePath() {
		return this.filePath;
	}
	
	// 等待写入硬盘的队列
	private ConcurrentLinkedQueue<FileData> writeDataQueue;
//	private AtomicInteger queueCnt = new AtomicInteger();
	// 队列元素超过多少就批量写硬盘
	private short threshHold = 0;
	// 当前这个文件是否可以写
	private volatile boolean canWrite = false;
	private volatile boolean stop = false;
	private volatile boolean syncAlready = true;
	
//	private ArrayList<FileData> sortDataList;
	private ExecutorService writeThread;
	
	public MyFileWriter(final String path, final short threshHold) {
		this.writeDataQueue = new ConcurrentLinkedQueue<FileData>();
		this.filePath = path;
		this.threshHold = threshHold;
//		sortDataList =  new ArrayList<>(500);
		this.writeThread = Executors.newSingleThreadExecutor();
//		this.writeThread =  Executors.newFixedThreadPool(10);
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				// sleep  120ms->60ms->30ms->15ms
				int millis = 120;
				boolean flag = false;
				while(!stop){
					try {
						Thread.sleep(millis);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if(stop) {
						break;
					}
					if(syncAlready) {
						syncAlready = true;
						millis = 120;
					}else{
					flag = sync();
						if(flag) {
							millis = 120;
						}else{
							millis >>= 1;
							if(millis < 10) {
								millis = 120;
							}
						}
					}
				}
			}
		}).start();
	}
	
	public void setCanWrite(final boolean flag) {
		this.canWrite = flag;
	}
	
	public void writeData(final FileData fd) {
		this.writeDataQueue.offer(fd);
//		int val = this.queueCnt.incrementAndGet();
//		MyLogger.info("In " + this.filePath + 
//				" FileWriter, writeData, pos=" + fd.position
//				+" len=" + (fd.data.limit()/1024) + " queueCnt=" + val);
		if(this.canWrite && this.writeDataQueue.size() >= threshHold) {
//			MyLogger.info(filePath +" fileWriter, queueCnt=" + val
//					+ " queueSize=" + this.writeDataQueue.size());
			this.writeThread.execute(new Runnable() {
				@Override
				public void run() {
					sync();
				}
			});
		}
	}
	
	public void forceSync() {
//		MyLogger.info("MyFileWriter forceSync writeDataQueue size=" + writeDataQueue.size());
		this.writeThread.execute(new Runnable() {
			@Override
			public void run() {
				sync();
			}
		});
	}
	public void forceSyncAndShutdown() {
		this.stop = true;
		this.writeThread.execute(new Runnable() {
			
			@Override
			public void run() {
				sync();
			}
		});
		
//		// 还是没写完
//		if(this.writeDataQueue.size() > 0) {
//			RandomAccessFile raf = null;
//			try {
//					raf = new RandomAccessFile(filePath, "rw");
//				} catch (FileNotFoundException e) {
//					e.printStackTrace();
//			}
//			if(raf == null) {
//					return;
//			}
//			FileChannel fc = raf.getChannel();
//			FileData fd = null;
//			ArrayList<FileData> sortDataList = ArrayListPool.take();
//			while((fd = this.writeDataQueue.poll()) != null) {
//				sortDataList.add(fd);
//			}
//			Collections.sort(sortDataList);
//			for(FileData fdata : sortDataList){
//				try {
//					fc.position(fdata.position);
//					Poolable<ByteBuffer> poolBuf = fdata.data;
//					ByteBuffer byteBuf = poolBuf.getObject();
//					while(byteBuf.hasRemaining()) {
//						fc.write(byteBuf);
//					}
//					if(byteBuf.isDirect()) {
////						DirectByteBuffPool.putBackSmallPool(fdata.data, fdata.dataSize);
//					}else{
//						ByteBufferPool.putBack(poolBuf);
//					}
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//			 ArrayListPool.putBack(sortDataList);
//			try {
//				fc.close();
//				raf.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		
//		}
		this.writeThread.shutdown();
		try {
			this.writeThread.awaitTermination(1L, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		MyLogger.info("forceSyncAndShutdown finish");
	}
	
	public synchronized boolean sync() {
//		MyLogger.info("this.writeDataQueue.size()=" +this.writeDataQueue.size());
		if(this.writeDataQueue.size() <= 0) {
//			MyLogger.info("this.writeDataQueue.size()=" +this.writeDataQueue.size());
			return false;
		}
		syncAlready = true;
		Poolable<ArrayList<FileData>> pal = ArrayListPool.take();
		if(pal == null) {
			return false;
		}
		ArrayList<FileData> sortDataList = pal.getObject();
		sortDataList.clear();
		FileData fd = null;
		while((fd = this.writeDataQueue.poll()) != null) {
			sortDataList.add(fd);
		}
		if(sortDataList.size() <= 0) {
			 ArrayListPool.putBack(pal);
			return false;
		}
//		int oldval = queueCnt.get();
//		this.queueCnt.set(oldval - sortDataList.size());
//		MyLogger.info("in MyFileWriter sync, sortDataList.size=" + sortDataList.size()
//		+" filePath="+filePath);
		Collections.sort(sortDataList);
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(filePath, "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		if(raf == null) {
			return false;
		}
		FileChannel fc = raf.getChannel();
		// 直接用raf 写
		for(FileData fdata : sortDataList){
			try {
				Poolable<ByteBuffer> poolBuf = fdata.data;
				fc.position(fdata.position);
				ByteBuffer byteBuf = poolBuf.getObject();
				while(byteBuf.hasRemaining()) {
					fc.write(byteBuf);
				}
				ByteBufferPool.putBack(poolBuf);
//				if(byteBuf.isDirect()) {
////					MyLogger.info("putBack DirectByteBuffer size=" + fdata.data.capacity()
////					+" fdata.dataSize=" + fdata.dataSize);
////					DirectByteBuffPool.putBackSmallPool(fdata.data, fdata.dataSize);
//				}else{
////					MyLogger.info("putBack ByteBuffer size=" + byteBuf.capacity()
////					+" fdata.dataSize=" + fdata.dataSize);
//					ByteBufferPool.putBack(poolBuf);
//				}
				fdata.data = null;
				fdata = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		 ArrayListPool.putBack(pal);
		 sortDataList = null;
		try {
			fc.close();
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return true;
	}
}