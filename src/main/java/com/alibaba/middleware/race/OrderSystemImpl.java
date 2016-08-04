package com.alibaba.middleware.race;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.omg.CORBA.Current;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.alibaba.middleware.race.OrderSystem.TypeException;
import com.cloudteam.jenson.ArrayListPool;
import com.cloudteam.jenson.BuyerGoodOrderEntry;
import com.cloudteam.jenson.BuyerGoodOrderEntryPool;
import com.cloudteam.jenson.ByteArrayBuffer;
import com.cloudteam.jenson.ByteBufferPool;
import com.cloudteam.jenson.DirectByteBuffPool;
import com.cloudteam.jenson.FileManager;
import com.cloudteam.jenson.FileWriterManager;
import com.cloudteam.jenson.GBMetaInfo;
import com.cloudteam.jenson.GBMetaInfoPool;
import com.cloudteam.jenson.GZIPCompression;
import com.cloudteam.jenson.GoodOrderEntry;
import com.cloudteam.jenson.MyHashMap;
import com.cloudteam.jenson.MyLogger;
import com.cloudteam.jenson.OrderMetaInfo;
import com.cloudteam.jenson.OrderMetaInfoPool;
import com.cloudteam.jenson.PrimeFinder;
import com.cloudteam.jenson.SimpleLRUCache;
import com.cloudteam.jenson.Utils;
import com.cloudteam.xiaode.ComparableKeys;
import com.cloudteam.xiaode.KeyValueImpl;
import com.cloudteam.xiaode.ResultImpl;
import com.cloudteam.xiaode.Row;
import com.haiwanwan.common.objectpool.Poolable;


public class OrderSystemImpl implements OrderSystem, ConstructFinishListener {
	
	private Lock disk1Lock = new ReentrantLock(false);
	private Lock disk2Lock = new ReentrantLock(false);
	private Lock disk3Lock = new ReentrantLock(false);
	
	private ExecutorService disk1Producer;
	private ExecutorService disk2Producer;
	private ExecutorService disk3Producer;
	
	
//	private int orderCnt = 20000000;
//	private int goodCnt = 1000000;
//	private int buyerCnt = 2000000;
	
	private int orderCnt = 400000;
	private int goodCnt = 4000;
	private int buyerCnt = 8000;
	
//	private int orderCnt = 400000000;
//	private int goodCnt = 4000000;
//	private int buyerCnt = 8000000;
	
//	private SimpleLRUCache lruCache = null;
	
	private FileManager fileManager;
	private MyHashMap<Long,   Poolable<OrderMetaInfo>> orderMetaInfoMap;
	private MyHashMap<byte[], Poolable<GBMetaInfo>> goodMetaInfoMap;
	private MyHashMap<byte[], Poolable<GBMetaInfo>> buyerMetaInfoMap;
	// 用于根据买家或者商品查询批量订单
	private MyHashMap<String, Poolable<BuyerGoodOrderEntry>> buyerOrderEntryMap;
	private MyHashMap<String, Poolable<BuyerGoodOrderEntry>> goodOrderEntryMap;
	
	private volatile boolean constructFinish = false;
	private AtomicInteger waitHashMapFinishCnt = new AtomicInteger();
	
	// 每个路径对应一个RandomAccessFile.需要一直打开，因为后面查询也需要
//	private HashMap<String, RandomAccessFile> readRAFMap;
	// 保存各种文件总共大小,unit: Byte
	private long totalOrderFilesLenB = 0L;
	private long totalBuyerFilesLenB = 0L;
	private long totalGoodFilesLenB = 0L;
	
	// 保存某单个文件的大小，单位:Byte,key=文件路径
	private HashMap<String, Long> fileSizeByteMap;
	
	private final int SIZE_16KB = (1<<14);		// 16KB
	private final int SIZE_32KB = (1<<15);		// 32kB
	private final int SIZE_64KB = (1<<16);		// 64kB
	private final int SIZE_128KB = (1<<17);		// 128kB
	private final int SIZE_256KB = (1<<18);		// 256kB
	
//	// 初始Block大小为16KB
//	private int initBlckSize = SIZE_16KB;
	// 订单大概每个block放1300个
	private int orderMapBlkSize = SIZE_32KB;
	// 商品大概每个block放256个
	private int goodMapBlkSize = SIZE_16KB;
	// 大概每个block放512个
	private int buyerMapBlkSize = SIZE_32KB;
	private int buyerOrderMapBlkSize = SIZE_256KB;
	private int goodOrderMapBlkSize = SIZE_256KB;
	
	// memory mapped 读buffer大小
	private final long READ_BUF_SIZE_16MB = (1<<24);		// 16MB 
	private final long READ_BUF_SIZE_32MB = (1<<25);		// 32MB 
	private final long READ_BUF_SIZE_64MB = (1<<26);		// 64MB
	private final long READ_BUF_SIZE_128MB = (1<<27);		// 128MB
	private final long READ_BUF_SIZE_256MB = (1<<28);		// 256MB
	private final long READ_BUF_SIZE_512MB = (1<<29);		// 512MB
	
	
	// HashBlock队列里的元素超过多少时会发生同步
//	private short orderMapThreshold = 150;
//	private short goodMapThreshold = 80;
//	private short buyerMapThreshold = 150;
//	private short goodOrderMapThreshold = 50;
//	private short buyerOrderMapThreshold = 20;
	private short orderMapThreshold = 5;
	private short goodMapThreshold = 5;
	private short buyerMapThreshold = 10;
	private short goodOrderMapThreshold = 50;
	private short buyerOrderMapThreshold = 50;
	
//	private ExecutorService threadsPool;
	private Thread constructThread;
	
	// 分别对应存在各个磁盘的各种输入文件路径
	private ArrayList<String> disk1OrderFiles;
	private ArrayList<String> disk2OrderFiles;
	private ArrayList<String> disk3OrderFiles;
	
	private ArrayList<String> disk1GoodFiles;
	private ArrayList<String> disk2GoodFiles;
	private ArrayList<String> disk3GoodFiles;
	private ArrayList<String> disk1BuyerFiles;
	private ArrayList<String> disk2BuyerFiles;
	private ArrayList<String> disk3BuyerFiles;
	
	
	public OrderSystemImpl() {
		
//		this.readRAFMap = new HashMap<String, RandomAccessFile>();
		this.fileSizeByteMap = new HashMap<String, Long>();
		
		this.disk1Producer = Executors.newSingleThreadExecutor();
		this.disk2Producer = Executors.newSingleThreadExecutor();
		this.disk3Producer = Executors.newSingleThreadExecutor();
		
		this.disk1OrderFiles = new ArrayList<>(20);
		this.disk2OrderFiles = new ArrayList<>(16);
		this.disk3OrderFiles = new ArrayList<>(16);
		
		this.disk1GoodFiles = new ArrayList<>(3);
		this.disk2GoodFiles = new ArrayList<>(2);
		this.disk3GoodFiles = new ArrayList<>(3);
		
		this.disk1BuyerFiles = new ArrayList<>(3);
		this.disk2BuyerFiles = new ArrayList<>(2);
		this.disk3BuyerFiles = new ArrayList<>(3);
		
		
		MyLogger.init();
		
	}
	
	/**
	 * 根据文件大小获得最优的MMap buffer大小
	 * @param fileSizeInMB 文件大小，单位:MB
	 * @return buffer大小，单位:Byte
	 */
	private long getBestMMapSize(final long fileSizeInB) {
//		long fileSizeInMB = fileSizeInB >>> 20;
		if(fileSizeInB < READ_BUF_SIZE_16MB) {
			return fileSizeInB;
		}
		// 由于有可能同时有3条线程读，为了保证内存消耗不大于1G，所以
		// mmap size不能太大
		if(fileSizeInB < READ_BUF_SIZE_32MB) {
			return READ_BUF_SIZE_16MB;
		}
		if(fileSizeInB < READ_BUF_SIZE_64MB) {
			return READ_BUF_SIZE_32MB;
		}
		return READ_BUF_SIZE_64MB;
//		if(fileSizeInB < READ_BUF_SIZE_128MB) {
//			return READ_BUF_SIZE_64MB;
//		}
//		return READ_BUF_SIZE_128MB;
//		if(fileSizeInMB < READ_BUF_SIZE_256MB) {
//			return READ_BUF_SIZE_128MB;
//		}
//		if(fileSizeInMB < READ_BUF_SIZE_512MB) {
//			return READ_BUF_SIZE_256MB;
//		}
//		return READ_BUF_SIZE_512MB;
	}
	
	
	
	
	/**
	 * 读取Good,Buyer,Order文件
	 * @param type 0表示是Good文件，1表示是Buyer文件，2表示是Order文件
	 * @param path
	 */
	private void readGBOFiles(final byte type, final String path) {
		final byte fileBit = fileManager.getReadFileBitByPath(path);
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(path, "r");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		if(raf == null) {
			MyLogger.info("读取文件:" + path + " 失败");
			return;
		}
		// 每条线程负责完整读完整个文件，所以不会出现并发问题
		FileChannel fileChannel = raf.getChannel();
		long fileSizeByte = fileSizeByteMap.get(path);
		long bestBufSizeByte = getBestMMapSize(fileSizeByte);
		long bufSizeByte = bestBufSizeByte;
		long remainByte = fileSizeByte;
		MyLogger.info("bestBufSizeByte for " + path +" is " + (bestBufSizeByte));
		// 当前文件已经读到哪个位置了
		long curPos = 0;
		while(remainByte > 0) {
			bufSizeByte = ((bestBufSizeByte < remainByte)? bestBufSizeByte : remainByte);
			MappedByteBuffer inBuf = null;
			try {
				inBuf = fileChannel.map(FileChannel.MapMode.READ_ONLY, 
						curPos, bufSizeByte);
			} catch (IOException e) {
				e.printStackTrace();
			}
			if(inBuf == null) {
				MyLogger.err("error!inBuf=null");
				try {
					fileChannel.close();
					raf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				return;
			}			
			int beginPos = 0;
			int lineBeginPos = beginPos;
			int endPos = 0;
			// 在映射的内存里是否有某行没有换行符，
			// 如果某行没有换行符，说明该行被截断了
			boolean notHaveLineBrk = true;
			boolean isPool = true;
			while(inBuf.hasRemaining()) {
				// 对于一个正确的内存映射来说，该映射的最后一个byte一定是'\n'
				notHaveLineBrk = true;
				byte c = inBuf.get();
				isPool = true;
				if(c == '\n') {
					// 一行结束
					endPos = inBuf.position();					
					// 回到初始位置
					inBuf.position(beginPos);
					int bufLen = endPos - beginPos - 1;
					isPool = true;
					Poolable<byte[]> poolByteBuf = null;
					if(ByteArrayBuffer.sizeFit(bufLen)){
//						MyLogger.info("ByteArrayBuffer prepare to lock, size=" + bufLen);
						poolByteBuf = ByteArrayBuffer.take(bufLen);
//						MyLogger.info("ByteArrayBuffer prepare unlock, size=" + bufLen);
					}
					byte[] myBytes = null;
					if(poolByteBuf == null) {
						MyLogger.info("ByteArrayBuffer is null!bufLen="+bufLen);
						myBytes = new byte[bufLen];
						isPool = false;
					}else{
						myBytes = poolByteBuf.getObject();
					}
					inBuf.get(myBytes, 0, bufLen);
					// 还有一字节'\n'
					inBuf.get();
					lineBeginPos = beginPos;
					beginPos = inBuf.position();
					// 该商品记录在文件中的起始位置是beginPos, 长度是:endPos-beginpos-1
					// 设置该Good记录的元数据
					int len = endPos - lineBeginPos - 1;
					long metainfo = 0;
					metainfo = Utils.setGBOFileBit(fileBit, metainfo);
					metainfo = Utils.setGBOOffsetBit(curPos + lineBeginPos, metainfo);
					metainfo = Utils.setGBOLenBit(len, metainfo);
					if(type == Utils.TYPE_ORDER) {
						// 先获取Buyer,Good对象
						Poolable<BuyerGoodOrderEntry> poolBuyer = BuyerGoodOrderEntryPool.poll();
						BuyerGoodOrderEntry buyerOrderEntry = null;
						if(poolBuyer == null) {
							this.buyerOrderEntryMap.forceSync();
							poolBuyer = BuyerGoodOrderEntryPool.take();
						}
						if(poolBuyer != null) {
							buyerOrderEntry = poolBuyer.getObject();
						}
						Poolable<BuyerGoodOrderEntry> poolGood = BuyerGoodOrderEntryPool.poll();
						BuyerGoodOrderEntry goodOrderEntry = null;
						if(poolGood == null) {
							this.goodOrderEntryMap.forceSync();
							poolGood = BuyerGoodOrderEntryPool.take();
						}
						if(poolGood != null) {
							goodOrderEntry = poolGood.getObject();
						}
						String id =  Utils.findOidBidGidInBytes(myBytes, buyerOrderEntry.id, goodOrderEntry.id, len);
						for(int i = 0; i < buyerOrderEntry.id.length; i++) {
							if(buyerOrderEntry.id[i] == -1) {
								// 长度
								buyerOrderEntry.idLen = (byte) i;
								break;
							}
						}
						for(int i = 0; i < goodOrderEntry.id.length; i++) {
							if(goodOrderEntry.id[i] == -1) {
								// 长度
								goodOrderEntry.idLen = (byte) i;
								break;
							}
						}
//						// TODO:测试Log
//						String buyerId = "";
//						try {
//							buyerId = new String(buyerOrderEntry.id, 0, buyerOrderEntry.idLen, "UTF-8");
//						} catch (UnsupportedEncodingException e) {
//							e.printStackTrace();
//						}
//						String goodId = "";
//						try {
//							goodId = new String(goodOrderEntry.id, 0, goodOrderEntry.idLen, "UTF-8");
//						} catch (UnsupportedEncodingException e) {
//							e.printStackTrace();
//						}
//						MyLogger.info("OrderId=" + id + " goodId="+goodId+" buyerId=" + buyerId);
						if(poolByteBuf != null) {
							ByteArrayBuffer.putBack(poolByteBuf);
						}
						myBytes = null;
						long orderid = Long.parseLong(id);
						buyerOrderEntry.orderId = orderid;
						goodOrderEntry.orderId = orderid;
						id = null;
						Poolable<OrderMetaInfo> poolOrder = OrderMetaInfoPool.poll();
						if(poolOrder == null) {
							this.orderMetaInfoMap.forceSync();
							poolOrder = OrderMetaInfoPool.take();
						}
						OrderMetaInfo orderMeta = null;
						if(poolOrder != null) {
							orderMeta = poolOrder.getObject();
						}						
						orderMeta.orderId = orderid;
						orderMeta.metaInfo = metainfo;
						this.orderMetaInfoMap.put(orderid, poolOrder);
						this.buyerOrderEntryMap.putByByteId(buyerOrderEntry.id, buyerOrderEntry.idLen, poolBuyer);
						this.goodOrderEntryMap.putByByteId(goodOrderEntry.id, goodOrderEntry.idLen, poolGood);
					}else if(type == Utils.TYPE_GOOD) {
						Poolable<GBMetaInfo> poolGB = GBMetaInfoPool.poll();
						if(poolGB == null) {
							this.goodMetaInfoMap.forceSync();
							poolGB = GBMetaInfoPool.take();
						}
						GBMetaInfo gbMeta = null;
						if(poolGB != null) {
							gbMeta = poolGB.getObject();
						}
						if(gbMeta == null) {
							gbMeta = new GBMetaInfo(metainfo);
						}
						gbMeta.metainfo = metainfo;
						gbMeta.idLen = Utils.findValInBytes(Utils.GOOD_GOODID, myBytes, gbMeta.id, len);
						if(poolByteBuf != null) {
							ByteArrayBuffer.putBack(poolByteBuf);
						}
						this.goodMetaInfoMap.putByByteId(gbMeta.id, gbMeta.idLen, poolGB);
					}else if(type == Utils.TYPE_BUYER) {
						Poolable<GBMetaInfo> poolGB = GBMetaInfoPool.poll();
						if(poolGB == null) {
							this.buyerMetaInfoMap.forceSync();
							poolGB = GBMetaInfoPool.take();
						}
						GBMetaInfo gbMeta = null;
						if(poolGB != null) {
							gbMeta = poolGB.getObject();
						}
						if(gbMeta == null) {
							gbMeta = new GBMetaInfo(metainfo);
						}
						gbMeta.metainfo = metainfo;
						gbMeta.idLen = Utils.findValInBytes(Utils.BUYER_BUYERID, myBytes, gbMeta.id, len);
						if(poolByteBuf != null) {
							ByteArrayBuffer.putBack(poolByteBuf);
						}
						this.buyerMetaInfoMap.putByByteId(gbMeta.id, gbMeta.idLen, poolGB);
					}
					notHaveLineBrk = false;
					
				}
			}
			if(notHaveLineBrk) {
				// 最后一个记录没有换行符，说明被截断了，把该行记录
				// 的数据“放回去”
				curPos = curPos + beginPos;
				long len = beginPos;
				remainByte -= len;
			}else{
				curPos += bufSizeByte;
				remainByte -= bufSizeByte;
			}
		}
		try {
			fileChannel.close();
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 读Good、Buyer文件
	 */
	private void readGBFiles() {	
		long stime = System.currentTimeMillis();
		// Buyer的索引文件存在disk3, 商品在disk2
		// 买家在disk1,disk2各有两个文件，所以先同时读这两个
		int countCnt = this.disk1BuyerFiles.size() + this.disk2BuyerFiles.size();
		final CountDownLatch latch1 = new CountDownLatch(countCnt);
		for(final String path : this.disk1BuyerFiles) {
			this.disk1Producer.submit(new Runnable() {
				@Override
				public void run() {
					MyLogger.info("开始读:" + path);
					long stime = System.currentTimeMillis();
					disk1Lock.lock();
					try{
						readGBOFiles(Utils.TYPE_BUYER ,path);
					}catch(Exception ex){
						ex.printStackTrace();
					}
					finally{
						
						disk1Lock.unlock();
					}
					long etime = System.currentTimeMillis();
					MyLogger.info("read " + path + " took:" + (etime-stime));
					latch1.countDown();
				}
			});
		}
		for(final String path : this.disk2BuyerFiles) {
			this.disk2Producer.submit(new Runnable() {
				@Override
				public void run() {
					MyLogger.info("开始读:" + path);
					long stime = System.currentTimeMillis();
					disk2Lock.lock();
					try{
						readGBOFiles(Utils.TYPE_BUYER ,path);
					}finally{
						disk2Lock.unlock();
					}
					long etime = System.currentTimeMillis();
					MyLogger.info("read " + path + " took:" + (etime-stime));
					latch1.countDown();
				}
			});
		}
		// 等待读完
		try {
			latch1.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// 现在买家只剩和它索引文件同在disk3的文件了。
		// 商品索引文件存在disk2,在disk1有2个输入文件，在disk3有一个。
		countCnt = this.disk1GoodFiles.size() + this.disk3GoodFiles.size();
		final CountDownLatch latch2 = new CountDownLatch(countCnt);
		for(final String path : this.disk1GoodFiles) {
			this.disk1Producer.submit(new Runnable() {
				@Override
				public void run() {
					MyLogger.info("开始读:" + path);
					long stime = System.currentTimeMillis();
					disk1Lock.lock();
					try{
						readGBOFiles(Utils.TYPE_GOOD ,path);
					}catch(Exception ex){
						ex.printStackTrace();
					}
					finally{
						disk1Lock.unlock();
					}
					long etime = System.currentTimeMillis();
					MyLogger.info("read " + path + " took:" + (etime-stime));
					latch2.countDown();
				}
			});
		}
		for(final String path : this.disk3GoodFiles) {
			this.disk3Producer.submit(new Runnable() {
				@Override
				public void run() {
					MyLogger.info("开始读:" + path);
					long stime = System.currentTimeMillis();
					disk3Lock.lock();
					try{
						readGBOFiles(Utils.TYPE_GOOD ,path);
					}finally{
						disk3Lock.unlock();
					}
					long etime = System.currentTimeMillis();
					MyLogger.info("read " + path + " took:" + (etime-stime));
					latch2.countDown();
				}
			});
		}
		// 等待读完
		try {
			latch2.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		// 现在买家只剩disk3的文件，商品只剩disk2的文件
		countCnt = this.disk3BuyerFiles.size() + this.disk2GoodFiles.size();
		final CountDownLatch latch3 = new CountDownLatch(countCnt);
		for(final String path : this.disk2GoodFiles) {
			this.disk2Producer.submit(new Runnable() {
				@Override
				public void run() {
					MyLogger.info("开始读:" + path);
					long stime = System.currentTimeMillis();
					disk2Lock.lock();
					try{
						readGBOFiles(Utils.TYPE_GOOD ,path);
					}finally{
						disk2Lock.unlock();
					}
					long etime = System.currentTimeMillis();
					MyLogger.info("read " + path + " took:" + (etime-stime));
					latch3.countDown();
				}
			});
		}
		for(final String path : this.disk3BuyerFiles) {
			this.disk3Producer.submit(new Runnable() {
				@Override
				public void run() {
					MyLogger.info("开始读:" + path);
					long stime = System.currentTimeMillis();
					disk3Lock.lock();
					try{
						readGBOFiles(Utils.TYPE_BUYER ,path);
					}finally{
						disk3Lock.unlock();
					}
					long etime = System.currentTimeMillis();
					MyLogger.info("read " + path + " took:" + (etime-stime));
					latch3.countDown();
				}
			});
		}
		// 等待读完
		try {
			latch3.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long etime = System.currentTimeMillis();
				
		MyLogger.info("readGBFiles take time " + (etime-stime) + " ms");
	}
	
	private void readOrderFiles() {
		
		long stime = System.currentTimeMillis();
		// 订单索引文件放在disk3，所以先读完disk1,disk2
//		int fileCnt = this.disk1OrderFiles.size() + this.disk2OrderFiles.size();
		int fileCnt = this.disk1OrderFiles.size() + this.disk2OrderFiles.size();
		final CountDownLatch disk12Lath = new CountDownLatch(fileCnt);
		for(final String disk1Path : this.disk1OrderFiles){
			this.disk1Producer.execute(new Runnable() {
			@Override
			public void run() {
					MyLogger.info("开始读文件:" + disk1Path);
					long stime = System.currentTimeMillis();
					disk1Lock.lock();
					try{
						readGBOFiles(Utils.TYPE_ORDER ,disk1Path);
					}finally{
						disk1Lock.unlock();
					}
					long etime = System.currentTimeMillis();
					MyLogger.info("read " + disk1Path + " took:" + (etime-stime));
					Utils.printCurMem();
					disk12Lath.countDown();
				}
			});	
		}
		for(final String disk2Path : this.disk2OrderFiles){
			// 读disk2
			this.disk2Producer.execute(new Runnable() {
				@Override
				public void run() {
						MyLogger.info("开始读文件:" + disk2Path);
						long stime = System.currentTimeMillis();
						disk2Lock.lock();
						try{
							readGBOFiles(Utils.TYPE_ORDER ,disk2Path);
						}finally{
							disk2Lock.unlock();
						}
						long etime = System.currentTimeMillis();
						MyLogger.info("read " + disk2Path + " took:" + (etime-stime));
						Utils.printCurMem();
						disk12Lath.countDown();
				}
			});	
		}
		try {
			disk12Lath.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		// 然后读disk3剩下的（文件较多，可能会读不完！)
		fileCnt = this.disk3OrderFiles.size();
		final CountDownLatch disk3Lath = new CountDownLatch(fileCnt);
		for(final String disk3Path : this.disk3OrderFiles){
			this.disk1Producer.execute(new Runnable() {
			@Override
			public void run() {
					MyLogger.info("开始读文件:" + disk3Path);
					long stime = System.currentTimeMillis();
					disk3Lock.lock();
					try{
						readGBOFiles(Utils.TYPE_ORDER ,disk3Path);
					}finally{
						disk3Lock.unlock();
					}
					long etime = System.currentTimeMillis();
					MyLogger.info("read " + disk3Path + " took:" + (etime-stime));
					disk3Lath.countDown();
				}
			});	
		}
		try {
			disk3Lath.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long etime = System.currentTimeMillis();
		MyLogger.info("readOrderFiles take:" + (etime-stime) + " ms");
		
	}
	private void constructData() {
		// 先并行读Good和Buyer信息，然后才读订单信息。
		readGBFiles();
		this.goodMetaInfoMap.forceSyncAndWait();
		this.buyerMetaInfoMap.forceSyncAndWait();
		
		FileWriterManager.forceSync();
		
		Utils.printCurMem();
		
		// sleep 2 sec
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		// 获得GoodCnt和BuyerCnt以后才构造这3个map
		int cap = this.orderCnt / 1000;
		// 对应的素数
		cap = (int) (cap * 1.1f);
		cap = PrimeFinder.findNxtSmallPrime(cap);
		this.orderMetaInfoMap = new MyHashMap<Long, Poolable<OrderMetaInfo>>(cap, this.orderMapBlkSize, 
				this.orderMapThreshold, Utils.TYPE_ORDER, fileManager, this);
		// 对应的素数
		cap = (int) (this.buyerCnt / 50);
		cap = (int) (cap * 1.1f);
		cap = PrimeFinder.findNxtSmallPrime(cap);
		this.buyerOrderEntryMap = new MyHashMap<String, Poolable<BuyerGoodOrderEntry>>(cap, this.buyerOrderMapBlkSize, 
				this.buyerOrderMapThreshold, Utils.TYPE_BUYER_ORDER, fileManager, this);
		
		// 对应的素数
		cap = (int) (this.goodCnt / 50);
		cap = (int) (cap * 1.1f);
		cap = PrimeFinder.findNxtSmallPrime(cap);
		this.goodOrderEntryMap = new MyHashMap<String,  Poolable<BuyerGoodOrderEntry>>(cap, this.goodOrderMapBlkSize,
				this.goodOrderMapThreshold, Utils.TYPE_GOOD_ORDER, fileManager, this);
		
		Utils.printCurMem();
		
		readOrderFiles();
		MyLogger.info("finish read files!");
		FileWriterManager.setAllCanWrite(true);
		
		this.disk1Producer.shutdown();
		this.disk2Producer.shutdown();
		this.disk3Producer.shutdown();
		
		
		// 构造完以后要告诉MyHashMap，让其强行对没写进Disk的元素同步
		this.orderMetaInfoMap.forceSyncAndWait();
		this.buyerOrderEntryMap.forceSyncAndWait();
		this.goodOrderEntryMap.forceSyncAndWait();
		FileWriterManager.forceSync();
//		this.goodOrderEntryMap.forceSync();
//		this.buyerOrderEntryMap.forceSync();
		
		MyLogger.info("waitConstructFinish begin");
//		try {
//			Thread.sleep(2000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
		goodMetaInfoMap.waitConstructFinish();
		buyerMetaInfoMap.waitConstructFinish();
		orderMetaInfoMap.waitConstructFinish();;
		buyerOrderEntryMap.waitConstructFinish();
		goodOrderEntryMap.waitConstructFinish();
		FileWriterManager.forceSyncAndWait();
		MyLogger.info("waitConstructFinish all done");
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				ByteBufferPool.clean();
				ByteArrayBuffer.clean();
				GBMetaInfoPool.clean();
				OrderMetaInfoPool.clean();
				BuyerGoodOrderEntryPool.clean();
				
			}
		}).start();
		
		this.fileManager.close();
		DirectByteBuffPool.cleanSmallPool();
		DirectByteBuffPool.initBigPool();
		constructFinish = true;
		MyLogger.info("FileWriterManager.forceSync() done");
		
		
		MyLogger.info("finish constructData!");
//		this.lruCache = new SimpleLRUCache(orderCnt, buyerCnt, goodCnt);
//		for(String path : this.readRAFMap.keySet()) {
//			try {
//				this.readRAFMap.get(path).close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//		this.readRAFMap.clear();
		
//		this.orderMetaInfoMap.setLRUCache(lruCache);
//		this.goodMetaInfoMap.setLRUCache(lruCache);
//		this.buyerMetaInfoMap.setLRUCache(lruCache);
//		// 其实这两个不需要缓存，因为只会调用它们的getMultiRow方法
//		this.goodOrderEntryMap.setLRUCache(lruCache);
//		this.buyerOrderEntryMap.setLRUCache(lruCache);
		
	}
	@Override
	public void construct(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) throws IOException,
			InterruptedException {
		this.fileManager = new FileManager(orderFiles, buyerFiles, goodFiles, 
				storeFolders);
		this.fileManager.setOrderMapBlkSize(this.orderMapBlkSize);
		this.fileManager.setGoodMapBlkSize(this.goodMapBlkSize);
		this.fileManager.setBuyerMapBlkSize(this.buyerMapBlkSize);
		this.fileManager.setGoodOrderMapBlkSize(this.goodOrderMapBlkSize);
		this.fileManager.setBuyerOrderMapBlkSize(this.buyerOrderMapBlkSize);
		this.fileManager.initData();
		
		HashMap<String, Short> totalOutFiles = new HashMap<>(40);
		// 获取输出文件
		/*
		    orderMapBlkSize = SIZE_128KB;
			goodMapBlkSize = SIZE_256KB;
			buyerMapBlkSize = SIZE_128KB;
			buyerOrderMapBlkSize = SIZE_64KB;
			goodOrderMapBlkSize = SIZE_128KB;
		 */
		Short thld = 50;
		for(String path : this.fileManager.getOrderBlockFilePath()){
			// 一个block有250B，200个48k了
			totalOutFiles.put(path, thld);
		}
		for(String path : this.fileManager.getBuyerBlockFilePath()){			
			totalOutFiles.put(path, thld);
		}
		for(String path : this.fileManager.getGoodOrderBlockFilePath()){			
			totalOutFiles.put(path, thld);
		}
		for(String path : this.fileManager.getGoodBlockFilePath()){		
			totalOutFiles.put(path, thld);
		}
		for(String path : this.fileManager.getBuyerOrderBlockFilePath()){		
			totalOutFiles.put(path, thld);
		}
		FileWriterManager.init(totalOutFiles);
		FileWriterManager.setAllCanWrite(true);
		
		for(String path : orderFiles) {
			if(path.charAt(Utils.diskTypePos) == '1') {
				this.disk1OrderFiles.add(path);
			}else if(path.charAt(Utils.diskTypePos) == '2') {
				this.disk2OrderFiles.add(path);
			}if(path.charAt(Utils.diskTypePos) == '3') {
				this.disk3OrderFiles.add(path);
			}
			RandomAccessFile raf = new RandomAccessFile(path, "r");
			long tmp = raf.length();			
			this.totalOrderFilesLenB += tmp;
			this.fileSizeByteMap.put(path, tmp);
//			MyLogger.info(path+" size:" + (tmp/1048576) + " MB");
			raf.close();
		}
		for(String path : buyerFiles) {
			if(path.charAt(Utils.diskTypePos) == '1') {
				this.disk1BuyerFiles.add(path);
			}else if(path.charAt(Utils.diskTypePos) == '2') {
				this.disk2BuyerFiles.add(path);
			}if(path.charAt(Utils.diskTypePos) == '3') {
				this.disk3BuyerFiles.add(path);
			}
			RandomAccessFile raf = new RandomAccessFile(path, "r");
			long tmp = raf.length();	
			this.totalBuyerFilesLenB += tmp;
			this.fileSizeByteMap.put(path, tmp);
//			MyLogger.info(path+" size:" + (tmp/1048576) + " MB");
			raf.close();
		}
	
		for(String path : goodFiles) {
			if(path.charAt(Utils.diskTypePos) == '1') {
				this.disk1GoodFiles.add(path);
			}else if(path.charAt(Utils.diskTypePos) == '2') {
				this.disk2GoodFiles.add(path);
			}if(path.charAt(Utils.diskTypePos) == '3') {
				this.disk3GoodFiles.add(path);
			}
			RandomAccessFile raf = new RandomAccessFile(path, "r");
			long tmp = raf.length();
			this.totalGoodFilesLenB += tmp;
			this.fileSizeByteMap.put(path, tmp);
//			MyLogger.info(path+" size:" + (tmp/1048576) + " MB");
			raf.close();
		}		
		MyLogger.info("totalOrderFilesLenB:" + (totalOrderFilesLenB) + " B");
		MyLogger.info("totalGoodFilesLenB" + (totalGoodFilesLenB) + " B");
		MyLogger.info("totalBuyerFilesLenB:" + (totalBuyerFilesLenB) + " B");
		
		if(totalOrderFilesLenB > 90000000000L) {
			this.orderCnt = 400000000;
			this.goodCnt = 4000000;
			this.buyerCnt = 8000000;
			
		}
		
		this.orderMapThreshold = 10;
		this.buyerMapThreshold = 6;
		this.goodMapThreshold = 6;
		this.goodOrderMapThreshold = 10;
		this.buyerOrderMapThreshold = 10;
		
		int cap = this.buyerCnt >> 9;
		// 对应的素数
		cap = (int) (cap * 1.1f);
				cap = PrimeFinder.findNxtSmallPrime(cap);
		MyLogger.info("best capability for buyerMetaInfoMap is:" + cap);
		this.buyerMetaInfoMap = new MyHashMap<byte[], Poolable<GBMetaInfo>>(cap,  this.buyerMapBlkSize, 
				this.buyerMapThreshold, Utils.TYPE_BUYER, fileManager, this);
//		// TODO:TEST log
//		this.buyerCap = cap;
//		this.buyerBlockCnt = new ConcurrentHashMap<>(cap);
		
		cap = this.goodCnt >> 8;
		// 对应的素数
		cap = (int) (cap * 1.1f);
		cap = PrimeFinder.findNxtSmallPrime(cap);
		MyLogger.info("商品容量:" + cap);
		this.goodMetaInfoMap = new MyHashMap<byte[], Poolable<GBMetaInfo>>(cap, this.goodMapBlkSize,
				this.goodMapThreshold, Utils.TYPE_GOOD, fileManager, this);
//		//TODO TEST
//		this.goodCap = cap;
//		this.goodBlockCnt = new ConcurrentHashMap<>(cap);
		
		DirectByteBuffPool.initSmallPool();
		ByteBufferPool.init();
		ByteArrayBuffer.init();
		OrderMetaInfoPool.init();
		GBMetaInfoPool.init();
		BuyerGoodOrderEntryPool.init();
		ArrayListPool.init();
		Utils.printCurMem();
		
		constructThread = new Thread(new Runnable() {
			
			@Override
			public void run() {
				// 开始读取数据
				constructData();
			}
		});
		constructThread.start();
		
		Thread.sleep(3466600);
		this.disk1OrderFiles.clear();
		this.disk1OrderFiles = null;
		this.disk2OrderFiles.clear();
		this.disk2OrderFiles = null;
		this.disk3OrderFiles.clear();
		this.disk3OrderFiles = null;
		
		this.disk1BuyerFiles.clear();
		this.disk1BuyerFiles = null;
		this.disk2BuyerFiles.clear();
		this.disk2BuyerFiles = null;
		this.disk3BuyerFiles.clear();
		this.disk3BuyerFiles = null;
		this.disk1GoodFiles.clear();
		this.disk1GoodFiles = null;
		this.disk2GoodFiles.clear();
		this.disk2GoodFiles = null;
		this.disk3GoodFiles.clear();
		this.disk3GoodFiles = null;
		
		
	}
	
	
	/**
	 *  范围查询只能从硬盘中获取，没办法缓存
	 * @param goodId
	 * @return
	 */
	public ArrayList<Long> getOrderByGoodId(String goodId) {
		return this.goodOrderEntryMap.getMultiRowsByKey(goodId);
	}
	
	/**
	 * 范围查询只能从硬盘中获取，没办法缓存
	 * @param buyerId
	 * @return
	 */
	public ArrayList<Long> getOrderByBuyerId(String buyerId) {
		return this.buyerOrderEntryMap.getMultiRowsByKey(buyerId);
	}
	/**
	 * 从硬盘中查询数据，应当在lruCache中miss时才调用这个方法
	 * @param orderId
	 * @return
	 */
	public Row getOrder(long orderId) {
		Row row = this.orderMetaInfoMap.getRowByKey(orderId);
//		this.lruCache.putOrder(orderId, row);
		return row;
	}
	/**
	 * 从硬盘中查询数据，应当在lruCache中miss时才调用这个方法
	 */
	public Row getBuyer(String buyerId) {
		byte[] bid = null;
		try {
			bid = buyerId.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		if(bid == null){
			return null;
		}
		Row row = this.buyerMetaInfoMap.getRowByKey(bid);
//		this.lruCache.putBuyer(buyerId, row);
		return row;
	}
	/**
	 * 从硬盘中查询数据，应当在lruCache中miss时才调用这个方法
	 */
	public Row getGood(String goodId) {
		byte[] bid = null;
		try {
			bid = goodId.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		if(bid == null){
			return null;
		}
		Row row = this.goodMetaInfoMap.getRowByKey(bid);
//		this.lruCache.putGood(goodId, row);
		return row;
	}
	
	/********** 抄官方Demo的   ******************/
	
	private HashSet<String> createQueryKeys(Collection<String> keys) {
	    if (keys == null) {
	      return null;
	    }
	    return new HashSet<String>(keys);
	  }
	 
	@Override
	public Result queryOrder(long orderId, Collection<String> keys) {
		// 还没构造完，先继续block
		short sleepCnt = 1;
//		while(this.waitHashMapFinishCnt.get() < 4) {
		while(constructFinish == false){
		}
	    Row query = new Row();
	    query.putKV("orderid", orderId);
	    Row orderData = null;
//	    orderData = this.lruCache.getOrder(orderId);
	    if(orderData == null) {
	    	orderData = getOrder(orderId);
	    }
	    if (orderData == null) {
		      return null;
		}
	    if(keys != null && keys.size() == 0) {
			return new ResultImpl(orderId, new Row());
		}
	    String buyerId = orderData.getKV("buyerid").valueAsString();
	    String goodId = orderData.getKV("goodid").valueAsString();
	    Row buyerData = null;
	    Row goodData = null;
//	    buyerData = this.lruCache.getBuyer(buyerId);
	    if(buyerData == null) {
	    	buyerData = getBuyer(buyerId);
	    }
//	    goodData = this.lruCache.getGood(goodId);
	    if(goodData == null) {
	    	goodData = getGood(goodId);
	    }
	    return ResultImpl.createResultRow(orderData, buyerData, goodData,
		        createQueryKeys(keys));
	}

	@Override
	public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
			String buyerid) {
		// 还没构造完，先继续block
		while(constructFinish == false){			
		}
		Row buyerData = null;
//		buyerData = this.lruCache.getBuyer(buyerid);
		if(buyerData == null) {
			buyerData = getBuyer(buyerid);
		}
		if(buyerData == null) {
			LinkedList<Result> resultList = new LinkedList<Result>();
			return resultList.iterator();
		}
		boolean descding = false;
		ArrayList<Long> orderIdList = this.getOrderByBuyerId(buyerid);
		if(orderIdList == null || orderIdList.size() == 0) {
			LinkedList<Result> resultList = new LinkedList<Result>();
			return resultList.iterator();
		}
		ArrayList<Row> orderRows = new ArrayList<>(orderIdList.size());
		for(Long oid : orderIdList) {
			orderRows.add(this.getOrder(oid));
		}
		final SortedMap<ComparableKeys, Row> orders = new TreeMap<ComparableKeys, Row>();
		for(Row row : orderRows) {
			orders.put(new ComparableKeys("createtime", row, descding), row);
		}
		
		Row queryStart = new Row();
	    queryStart.putKV("createtime", startTime);
	    Row queryEnd = new Row();
	    queryEnd.putKV("createtime", endTime - 1); // exclusive end
	    final SortedMap<ComparableKeys, Row> resultMap = orders.subMap(
	    		new ComparableKeys("createtime", queryStart, descding), 
				new ComparableKeys("createtime", queryEnd, descding));
	    final LinkedList<Result> resultList = new LinkedList<Result>();
		// 查询每一个order对应的完整数据
		for(Row row : resultMap.values()) {
			// 查询Good
			String goodId = row.getKV("goodid").valueAsString();
			Row goodData = null;
//			goodData = this.lruCache.getGood(goodId);
			if(goodData == null) {
				goodData = getGood(goodId);
			}
			resultList.add(ResultImpl.createResultRow(row, buyerData, goodData, null));
		}
		return resultList.descendingIterator();
	}

	@Override
	public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
			Collection<String> keys) {
		// 还没构造完，先继续block
		while(constructFinish == false){			
		}
		Row goodData = null;
//		goodData = this.lruCache.getGood(goodid);
		if(goodData == null) {
			goodData = getGood(goodid);
		}
		if(goodData == null) {
			LinkedList<Result> resultList = new LinkedList<Result>();
			return resultList.iterator();
		}
		// 升序
		boolean descding = false;
		ArrayList<Long> orderIdList = this.getOrderByGoodId(goodid);
		if(orderIdList == null) {
			LinkedList<Result> resultList = new LinkedList<Result>();
			return resultList.iterator();
		}
		ArrayList<Row> orderRows = new ArrayList<Row>(orderIdList.size());
		// 一个一个去获取Order
		for(Long oid : orderIdList) {
			orderRows.add(this.getOrder(oid));
		}
		final SortedMap<ComparableKeys, Row> orders = new TreeMap<ComparableKeys, Row>();
		for(Row row : orderRows) {
			orders.put(new ComparableKeys("orderid", row, descding), row);
		}
		if(keys != null && keys.size() == 0) {
			 return new Iterator<OrderSystem.Result>() {

			      SortedMap<ComparableKeys, Row> o = new TreeMap<ComparableKeys, Row>(orders);

			      public boolean hasNext() {
			        return o != null && o.size() > 0;
			      }

			      public Result next() {
			        if (!hasNext()) {
			          return null;
			        }
			        ComparableKeys firstKey = o.firstKey();
			        Row orderData = o.get(firstKey);
			        o.remove(firstKey);
			        long orderid;
					try {
						orderid = orderData.getKV("orderid").valueAsLong();
						return new ResultImpl(orderid, new Row());
					} catch (TypeException e) {
						e.printStackTrace();
					}
					return null;
			      }

			      public void remove() {
			        // ignore
			      }
			    };
		}
		
		final ArrayList<Result> resultList = new ArrayList<Result>(orders.size() + 5);
		// 查询每一个order对应的完整数据
		for(Row row : orders.values()) {
			// 查询Good
			String buyerId = row.getKV("buyerid").valueAsString();
			Row buyerData = null;
//			buyerData = this.lruCache.getBuyer(buyerId);
			if(buyerData == null) {
				buyerData = getBuyer(buyerId);
			}
			resultList.add(ResultImpl.createResultRow(row, buyerData, goodData,
		        createQueryKeys(keys)));
		}
		orders.clear();
		return resultList.iterator();
	}

	@Override
	public KeyValue sumOrdersByGood(final String goodid, String key) {
		// 还没构造完，先继续block
		while(constructFinish == false){			
		}
		Row goodData = null;
		if(goodData == null) {
			goodData = getGood(goodid);
		}
		 if(goodData == null) {
		    	return null;
		    }
		HashSet<String> queryingKeys = new HashSet<String>();
	    queryingKeys.add(key);
	    ArrayList<Long> orderIdList = this.getOrderByGoodId(goodid);
	    if(orderIdList == null || orderIdList.size() == 0) {
	    	return null;
	    }
	    ArrayList<Row> orders = new ArrayList<Row>(orderIdList.size());
	    for(Long id : orderIdList) {
	    	orders.add(this.getOrder(id));
	    }
	    LinkedList<ResultImpl> allData = new LinkedList<ResultImpl>();
	    // 既然是对同一款商品的查询，为啥还要不停查询GOod??
	    for(Row row : orders) {
			// 查询Good
			String buyerId = row.getKV("buyerid").valueAsString();
//			String goodId = row.getKV("goodid").valueAsString();
			
			Row buyerData = null;
//			buyerData = this.lruCache.getBuyer(buyerId);
			if(buyerData == null) {
				buyerData = getBuyer(buyerId);
			}
			allData.add(ResultImpl.createResultRow(row, buyerData, goodData,
		        createQueryKeys(queryingKeys)));
		}
	    // accumulate as Long
	    try {
	      boolean hasValidData = false;
	      long sum = 0;
	      for (ResultImpl r : allData) {
	        KeyValue kv = r.get(key);
	        if (kv != null) {
	          sum += kv.valueAsLong();
	          hasValidData = true;
	        }
	      }
	      if (hasValidData) {
	        return new KeyValueImpl(key, Long.toString(sum));
	      }
	    } catch (TypeException e) {
	    }

	    // accumulate as double
	    try {
	      boolean hasValidData = false;
	      double sum = 0;
	      for (ResultImpl r : allData) {
	        KeyValue kv = r.get(key);
	        if (kv != null) {
	          sum += kv.valueAsDouble();
	          hasValidData = true;
	        }
	      }
	      if (hasValidData) {
	        return new KeyValueImpl(key, Double.toString(sum));
	      }
	    } catch (TypeException e) {
	    }

	    return null;
	}
	
	public void close() {
//		for(String key : readRAFMap.keySet()) {
//			try {
//				readRAFMap.get(key).close();
//			} catch (IOException e) {
//				// TODO 自动生成的 catch 块
//				e.printStackTrace();
//			}
//		}
		fileManager.close();
		this.disk1Producer.shutdown();
		this.disk2Producer.shutdown();
		this.disk3Producer.shutdown();
		
		this.orderMetaInfoMap.close();
		this.goodMetaInfoMap.close();
		this.buyerMetaInfoMap.close();
		this.goodOrderEntryMap.close();
		this.buyerOrderEntryMap.close();
		
		MyLogger.close();
	}

	@Override
	public void constructFinish() {
		this.waitHashMapFinishCnt.incrementAndGet();
	}

}