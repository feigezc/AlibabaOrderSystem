package com.cloudteam.jenson;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.ConstructFinishListener;
import com.cloudteam.xiaode.Row;


/**
 * 在hashmap里，通过hash将id对应到相应的Block. Block逻辑上存着
 * 同一个hash的数据的元信息，但其实每个Block只是一个Long表示的元信息，
 * 保存着该块的数据在文件中的位置。而块中的数据也不是真正的数据，也只是
 * 保存真实的数据在哪个文件中。
 * @author Jenson
 * @param <K> Long或者String
 * @param <V> OrderMetaInfo或者GBMetaInfo
 */
public final class MyHashMap<K,V> implements Serializable, MyListener<V>{
	private static final long serialVersionUID = 362498820763181265L;
	
	private ConstructFinishListener constructFinishListener;
	
	// capability of this hashmap
	private int capability;
	public int getCapability() {
		return this.capability;
	}
	// 每个块的大小。HashBlock的metainfo里面保存的块的长度可能是会增大的。
	public int initBlockSize;	
	private FileManager fileManager;
	// 表示该hashmap的类型是TYPE_ORDER还是TYPE_GOOD还是TYPE_BUYER
	private final byte mapType;
	private HashBlock<V> blocks[];
	private ExecutorService threadPool;
	
	private short threshold = 0;
	private volatile boolean syncAlready = true;
	private volatile boolean stop = false;
	// 用来保存该Block存储的文件。应一直维持打开，因为在后面查询时也需要
//	private HashMap<String, RandomAccessFile> blockRafMap = new HashMap<String, RandomAccessFile>();
//	private HashMap<String, RandomAccessFile> dataRafMap = new HashMap<String, RandomAccessFile>();
	/**
	 * 
	 * @param cap 容量 必须是素数
	 * @param blockSize 块大小
	 * @param thresh  阈值，超过该值才将数据写入磁盘
	 * @param type	该Map属于什么类型，值是:Utils.TYPE_ORDER,Utils.TYPE_GOOD等
	 * @param fileMng 文件管理器，整个程序共享一个
	 */
	public MyHashMap(final int cap, final int blockSize, final short thresh,
			final byte type, final FileManager fileMng, final ConstructFinishListener cfl) {
//		this.capability = PrimeFinder.nextPrime(cap);
		this.capability = cap;
		initBlockSize = blockSize;
		this.blocks = new HashBlock[this.capability];
		this.threshold = thresh;
//		this.threadPool = Executors.newFixedThreadPool(20);
		this.threadPool = Executors.newSingleThreadExecutor();
		this.mapType = type;
		this.fileManager = fileMng;
		this.constructFinishListener = cfl;
		MyLogger.info("mapType:" + mapType + " capability:" + capability);
		initHashBlocks();
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				// 100ms->50ms->25ms->17ms
				int millis = 100;
				while(!stop) {
					try {
						Thread.sleep(millis);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					if(stop) {
						break;
					}
					if(syncAlready) {
						syncAlready = false;
						millis = 100;
					}else{
//						MyLogger.info("MyHashMap wake up and force sync");
						forceSync();
						syncAlready = false;
						millis >>= 1;
						if(millis < 10) {
							millis = 100;
						}
					}
				}
			}
		}).start();
	}
	
	private void initHashBlocks() {
//		if(mapType == Utils.TYPE_BUYER_ORDER
//				|| mapType == Utils.TYPE_GOOD_ORDER) {
//			return;
//		}
		// 使用6条线程
		int threadCnt = 6;
		// 平均每条线程分得这么多个
		int avgCnt = capability / threadCnt;
		int curIdx = 0;
		long stime = System.currentTimeMillis();
		final CountDownLatch latch = new CountDownLatch(threadCnt);
		for(int i = 0; i < threadCnt; i++) {
			final int from = curIdx;
			int to = curIdx + avgCnt;
			if(to > capability) {
				to = capability;
			}
			if(i == threadCnt - 1) {
				// 最后一个了，要把剩下的都干完
				if(to < capability) {
					to = capability;
				}
			}
			final int end = to;
			// 每个线程分配的区间是:[from,end);
			curIdx = end;
			this.threadPool.execute(new Runnable() {
				@Override
				public void run() {
					for(int i = from; i < end; i++) {
						byte fileBit = 0;
						if(mapType == Utils.TYPE_ORDER) {
							fileBit = fileManager.getOrderBlockFileBit(i);
						}else if(mapType == Utils.TYPE_GOOD) {
							fileBit = fileManager.getGoodBlockFileBit(i);
						}else if(mapType == Utils.TYPE_BUYER) {
							fileBit = fileManager.getBuyerBlockFileBit(i);
						}else if(mapType == Utils.TYPE_BUYER_ORDER) {
							fileBit = fileManager.getBuyerOrderBlockFileBit(i);
						}else if(mapType == Utils.TYPE_GOOD_ORDER) {
							fileBit = fileManager.getGoodOrderBlockFileBit(i);
						}
							
						String fileName = "";
						if(mapType == Utils.TYPE_ORDER) {
							fileName = fileManager.getOrderBlockFileByBit(fileBit);
						}else if(mapType == Utils.TYPE_GOOD) {
							fileName = fileManager.getGoodBlockFileByBit(fileBit);
						}else if(mapType == Utils.TYPE_BUYER) {
							fileName = fileManager.getBuyerBlockFileByBit(fileBit);
						}else if(mapType == Utils.TYPE_GOOD_ORDER) {
							fileName = fileManager.getGoodOrderBlockFileByBit(fileBit);
						}else if(mapType == Utils.TYPE_BUYER_ORDER) {
							fileName = fileManager.getBuyerOrderBlockFileByBit(fileBit);
						}
						MyFileWriter mfw = FileWriterManager.getFileWriter(fileName);
						long offset = fileManager.getBlckFileOneOffset(fileName);
						blocks[i] = new HashBlock<V>(fileBit, offset, initBlockSize, 
								mfw, MyHashMap.this, fileName, threshold);
					}
					latch.countDown();
				}
			});
			
		}		
		try {
			latch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		long etime = System.currentTimeMillis();
		MyLogger.info("初始化Block花费:" + (etime-stime) + " ms");
	}
	

	/**
	 * 根据商品ID或者买家ID返回所有的订单数据
	 * @param key 商品ID或者买家ID
	 * @return 对应该key的订单ID的集合
	 */
	public ArrayList<Long> getMultiRowsByKey(K key) {
		ArrayList<Long> idList = new ArrayList<Long>(200);
		String id = (String)key;
		byte[] idBytes = null;
		try {
			idBytes = id.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e2) {
			e2.printStackTrace();
		}
		int h = Utils.hashCode(idBytes, idBytes.length);
		if(h<0) {
			h = Math.abs(h);
		}
		int curPos = h%this.capability;
		HashBlock<V> block = blocks[curPos];
		if(block == null) {
			return null;
		}	
		curPos = 0;
		long blkMetaInfo = block.getMetaInfo();
		// 然后从blkMetaInfo获取一个块的元数据
		final byte fileBit = Utils.getBlockFile(blkMetaInfo);
		String filePath = "";
		if(this.mapType == Utils.TYPE_GOOD_ORDER) {
			filePath = fileManager.getGoodOrderBlockFileByBit(fileBit);
		}else if(this.mapType == Utils.TYPE_BUYER_ORDER) {
			filePath = fileManager.getBuyerOrderBlockFileByBit(fileBit);
		}
		final long offset = Utils.getBlockOffset(blkMetaInfo);
		final int blkLen = (int) Utils.getBlockLen(blkMetaInfo);
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(filePath, "rw");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		if(raf == null) {
			MyLogger.info("raf is null!!");
			return null;
		}
		FileChannel chanel = raf.getChannel();
		boolean isDirect = true;
		ByteBuffer dataBuf = null;
		dataBuf = DirectByteBuffPool.takeBigPool(blkLen);
		if(dataBuf == null) {
			isDirect = false;
			dataBuf = ByteBuffer.allocate(blkLen);
		}
		dataBuf.clear();
		try {
			chanel.position(offset);
			chanel.read(dataBuf);
		} catch (IOException e) {
			e.printStackTrace();
			if(raf != null) {
				try {
					raf.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			if(isDirect) {
				DirectByteBuffPool.putBackBigPool(dataBuf, blkLen);
			}
			return null;
		}
		dataBuf.flip();
		
//		// 用mmap
//		MappedByteBuffer mmapBuffer = null;
//		try {
//			mmapBuffer = chanel.map(FileChannel.MapMode.READ_WRITE, 
//					offset, blkLen);
//		} catch (IOException e1) {
//			e1.printStackTrace();
//		}
//		if(mmapBuffer == null) {
//			return null;
//		}
		
		if(this.mapType == Utils.TYPE_GOOD_ORDER
			|| this.mapType == Utils.TYPE_BUYER_ORDER) {
			// 根据商品ID或者买家ID获取所有的订单信息
			/**
			 * 该entry在文件中的布局应为:
			 * 1byt表示数据开始+ 1byte表示买家、商品ID的长度 + 不定长的买家、商品id + 8byte的订单ID.
			 */
			byte correctBeginByte = BuyerGoodOrderEntry.beginByte;
			
			byte idLen = -1;
			byte[] readIdByte = null;
			long orderId = 0;
			while(dataBuf.hasRemaining()) {
				byte beginByte = dataBuf.get();
				if(beginByte != correctBeginByte) {
//					MyLogger.info("Order Bad data! Begin Byte should be " 
//							+ correctBeginByte + " bug get " + beginByte);
					continue;
				}
				if(dataBuf.remaining() <= 2) {
					break;
				}
				idLen = dataBuf.get();
				readIdByte = new byte[idLen];
				if(dataBuf.remaining() < idLen + 8) {
					break;
				}
				dataBuf.get(readIdByte);
				orderId = dataBuf.getLong();
				if(Arrays.equals(readIdByte, idBytes)) {
//					MyLogger.info("add orderId="+orderId);
					idList.add(orderId);
				}
			}
		}
		if(raf != null) {
			try {
				raf.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		if(isDirect) {
			DirectByteBuffPool.putBackBigPool(dataBuf, blkLen);
		}
		return idList;
	}
	
	/**
	 * 根据某个键返回一行Row
	 * @param key
	 * @return
	 */
	public Row getRowByKey(K key) {
		String line = "";
		int h = 0;
		if(key instanceof byte[]) {
			byte[] bkey = (byte[])key;
			h = Arrays.hashCode(bkey);
			if(h<0) {
				h = Math.abs(h);
			}
		}else{
			h = hash(key);
		}
		long blkMetaInfo = blocks[h%capability].getMetaInfo();
		// 然后从blkMetaInfo获取一个块的元数据
		byte fileBit = Utils.getBlockFile(blkMetaInfo);
		String filePath = "";
		if(this.mapType == Utils.TYPE_ORDER) {
			filePath = fileManager.getOrderBlockFileByBit(fileBit);
		}else if(this.mapType == Utils.TYPE_GOOD) {
			filePath = fileManager.getGoodBlockFileByBit(fileBit);
		}else if(this.mapType == Utils.TYPE_BUYER) {
			filePath = fileManager.getBuyerBlockFileByBit(fileBit);
		}
		long offset = Utils.getBlockOffset(blkMetaInfo);
		int blkLen = (int) Utils.getBlockLen(blkMetaInfo);
		boolean isDirect = false;
		ByteBuffer dataBuf = null;
		dataBuf = DirectByteBuffPool.takeBigPool(blkLen);
		if(dataBuf == null) {
			dataBuf = ByteBuffer.allocate(blkLen);
		}else{
			isDirect = true;
		}
		dataBuf.clear();
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(filePath, "rw");
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		if(raf == null) {
			if(isDirect) {
				DirectByteBuffPool.putBackBigPool(dataBuf, blkLen);
			}
			return null;
		}
		FileChannel chanel = raf.getChannel();
		try {
			chanel.position(offset);
			chanel.read(dataBuf);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		dataBuf.flip();
		// 找到对应于key的某个行数据的metainfo
		if(this.mapType == Utils.TYPE_ORDER) {
			long id = (Long)key;
			long orderID = -1;
			long orderMetaInfo = -1;
			// 布局是: 1byte + 8Byte的Long表示orderId + 8Byte的MetaInfo
			final byte orderBeginByte = OrderMetaInfo.beginByte;
			while(dataBuf.hasRemaining()) {
				byte beginByte = dataBuf.get();
				if(beginByte != orderBeginByte) {
					break;
				}
				orderID = dataBuf.getLong();
				orderMetaInfo = dataBuf.getLong();
				if(orderID == id) {
					break;
				}
			}
			if(orderID < 0 || orderID != id) {
				MyLogger.info("找不到:" + key);
				return null;
			}
			// 这时才是真的去读这一行的数据....
			line = readRowLineByMetaInfo(orderMetaInfo);
		}else{
			// 商品、买家信息在文件中布局和订单信息不一样
			// 这两种数据在硬盘中的布局为:
			// 1字节BeginByte + 1 字节Id长度 + N字节ID + 8字节metainfo 
			byte[] id = (byte[])key;
			byte idLen = -1;
			byte[] idBytes = null;
			long gbMetaInfo = -1;
			final byte gbBeginByte = GBMetaInfo.beginByte;
			while(dataBuf.hasRemaining()) {
				byte beginByte = dataBuf.get();
				if(beginByte != gbBeginByte) {
					break;
				}
				idLen = dataBuf.get();
				byte[] tmpIdBytes = new byte[idLen];
				dataBuf.get(tmpIdBytes);
				gbMetaInfo = dataBuf.getLong();
				// 是否是要找的
				if(Arrays.equals(tmpIdBytes, id)) {
					idBytes = tmpIdBytes;
					break;
				}
			}
			if(idBytes == null) {
				MyLogger.info("找不到:" + key);
				return null;
			}
			// 这时才是真的去读这一行的数据....
			line = readRowLineByMetaInfo(gbMetaInfo);
		}
//		MyLogger.info("key:" + key + " line:" + line);
		return Utils.createKVMapFromLine(line);
	}
	
	
	
	/**
	 * 根据key找到对应的block
	 * @param key
	 * @return
	 */
	private HashBlock<V> getBlock(K key) {
		if(this.mapType != Utils.TYPE_BUYER_ORDER &&
				this.mapType != Utils.TYPE_GOOD_ORDER) {
			int h = hash(key);
			// 当某个Block缓存的数目超过threshold时，该HashBlock
			// 会将数据写到Disk.但是同步工作不能在HashBlock使用线程，
			// 否则就是每个 HashBlock都用了一条线程，可能会有数以百万线程
			return blocks[h%capability];
		}else{
			// TYPE_BUYER_ORDER 和 TYPE_GOOD_ORDER 需要保证不会将不同的id
			// 放到同一个block，使用线性探测法能够确保一定能找到一个位置
			// TODO：看看算法导论对比下。。。
			String id = (String) key;
			int h = hash(key);
			int curPos = h%capability;
			HashBlock<V> block = blocks[curPos];
			String blkId = block.getId();
			while(blkId == null || !blkId.equals(id)) {
				curPos++;
				curPos %= capability;
				block = blocks[curPos];
				blkId = block.getId();
			}
			return block;
		}
	}
	
	private HashBlock<V> initBlock(int pos) {
		byte fileBit = 0;
		if(mapType == Utils.TYPE_BUYER_ORDER) {
			fileBit = fileManager.getBuyerOrderBlockFileBit(pos);
		}else if(mapType == Utils.TYPE_GOOD_ORDER) {
			fileBit = fileManager.getGoodOrderBlockFileBit(pos);
		}else {
			return null;
		}
//		MyLogger.info("mapType:" + mapType + " i=" + i);
		String fileName = "";
		
		if(mapType == Utils.TYPE_GOOD_ORDER) {
			fileName = fileManager.getGoodOrderBlockFileByBit(fileBit);
		}else if(mapType == Utils.TYPE_BUYER_ORDER) {
			fileName = fileManager.getBuyerOrderBlockFileByBit(fileBit);
		}
		long offset = fileManager.getBlckFileOneOffset(fileName);
		MyFileWriter mfw = FileWriterManager.getFileWriter(fileName);
		return new HashBlock<V>(fileBit, offset, initBlockSize, 
				mfw, MyHashMap.this, fileName, threshold);
	}
	
	
//	/**
//	 * 通过二次探查法找到一个唯一的位置放Key-val
//	 * @param key
//	 * @param val
//	 */
//	private void putByQuadProb(K key, V val) {
//		// TYPE_BUYER_ORDER 和 TYPE_GOOD_ORDER 需要保证不会将不同的id
//		// 放到同一个block，使用平方探测法
//		// TODO：看看算法导论对比下。。。
//		String id = (String) key;
//		int h = hash(key);
//		int curPos = h%capability;
//		HashBlock<V> block = blocks[curPos];	
//		if(block == null) {
//			// 说明是空的
//			block = initBlock(curPos);
//			block.setId(id);
//			blocks[curPos] = block;
//			block.add(val);
////			MyLogger.info("mapType=" + this.mapType 
////					+ " key:" + key + " 初始curPos:" + curPos +" 一次放入");
//		}else if(block.getId().equals(id)){
//			// 不为空，但是blkid=id
//			block.add(val);
//		}else{
////			MyLogger.info("mapType=" + this.mapType + " key:" + key 
////					+ " curPos=" + curPos
////					+ " 一次放入失败,当前元素个数:" + eleCntVal);
//			// 冲突次数
//			int collisNum = 1;
//			while(block != null) {
////				curPos++;
////				curPos %= capability;
//				curPos += (collisNum << 1) - 1;
//				collisNum++;
//				if(curPos >= capability) {
//					curPos %= capability;
//				}
//				block = blocks[curPos];
//				if(block != null) {
//					String blkId = block.getId();
//					if(blkId.equals(id)) {
//						break;
//					}
//				}
//			}
//			// 找到一个位置，有可能是空位，有可能是同一个id的位
//			if(block == null) {
//				block = initBlock(curPos);
//				block.setId(id);
//				blocks[curPos] = block;
//				block.add(val);
//			} else{
////				block.setId(id);
//				block.add(val);
//			}
//		}
//	}
//	
//	/**
//	 * 按照算法导论里面的二次探查法
//	 * @param key
//	 * @param val
//	 */
//	private void putByDoubleHash(K key, V val) {
//		// TYPE_BUYER_ORDER 和 TYPE_GOOD_ORDER 需要保证不会将不同的id
//				// 放到同一个block，使用平方探测法
//				// TODO：看看算法导论对比下。。。
//				String id = (String) key;
//				int h = hash(key);
//				int curPos = h%capability;
//				HashBlock<V> block = blocks[curPos];	
//				if(block == null) {
//					// 说明是空的
//					block = initBlock(curPos);
//					block.setId(id);
//					blocks[curPos] = block;
//					block.add(val);
////					MyLogger.info("mapType=" + this.mapType 
////							+ " key:" + key + " 初始curPos:" + curPos +" 一次放入");
//				}else if(block.getId().equals(id)){
//					// 不为空，但是blkid=id
//					block.add(val);
//				}else{
////					MyLogger.info("mapType=" + this.mapType + " key:" + key 
////							+ " curPos=" + curPos
////							+ " 一次放入失败,当前元素个数:" + eleCntVal);
//					// 冲突次数
//					int collisNum = 1;
//					int c1 = 2, c2 = 4;
//					while(block != null) {
////						curPos++;
////						curPos %= capability;
//						curPos += (collisNum<<1 + (collisNum*collisNum)<<2);
//						collisNum++;
//						if(curPos >= capability) {
//							curPos -= capability;
//						}
//						block = blocks[curPos];
//						if(block != null) {
//							String blkId = block.getId();
//							if(blkId.equals(id)) {
//								break;
//							}
//						}
//					}
//					// 找到一个位置，有可能是空位，有可能是同一个id的位
//					if(block == null) {
//						block = initBlock(curPos);
//						block.setId(id);
//						blocks[curPos] = block;
//						block.add(val);
//					} else{
////						block.setId(id);
//						block.add(val);
//					}
//				}
//	}
	
	public void putByByteId(final byte[] key, final byte keyLen, final V val) {
		int h = Utils.hashCode(key, keyLen);
		if(h<0) {
			h = Math.abs(h);
		}
		// 当某个Block缓存的数目超过threshold时，该HashBlock
		// 会将数据写到Disk.但是同步工作不能在HashBlock使用线程，
		// 否则就是每个 HashBlock都用了一条线程，可能会有数以百万线程
		blocks[h%capability].add(val, this.mapType);
//		if(this.mapType != Utils.TYPE_BUYER_ORDER &&
//				this.mapType != Utils.TYPE_GOOD_ORDER) {
//			int h = Utils.hashCode(key, keyLen);
//			if(h<0) {
//				h = Math.abs(h);
//			}
//			// 当某个Block缓存的数目超过threshold时，该HashBlock
//			// 会将数据写到Disk.但是同步工作不能在HashBlock使用线程，
//			// 否则就是每个 HashBlock都用了一条线程，可能会有数以百万线程
//			blocks[h%capability].add(val);
//		}else{
////			synchronized(this){
////				putByQuadProb(key, val);
////			}
//		}
	}
	
	public void put(K key, V val) {
		if(this.mapType != Utils.TYPE_BUYER_ORDER &&
				this.mapType != Utils.TYPE_GOOD_ORDER) {
			int h = hash(key);
			// 当某个Block缓存的数目超过threshold时，该HashBlock
			// 会将数据写到Disk.但是同步工作不能在HashBlock使用线程，
			// 否则就是每个 HashBlock都用了一条线程，可能会有数以百万线程
			blocks[h%capability].add(val, this.mapType);
		}else{
//			synchronized(this){
//				putByQuadProb(key, val);
////				putByDoubleHash(key, val);
//			}
		}
	}
	
	public int hash(Object key) {
	   int h = key.hashCode();
	   // make sure not negative
	   if(h < 0) {
		   h = Math.abs(h);	
	   }
	   return (key == null) ? 0 : (h) ;
	}
		
	/**
	 * used by double hash
	 * @param h
	 * @return
	 */
	public int hash2(int h, int m) {
//		int m = this.capability>>1 + 1;
		return 1+(h%(m-1));
	}

	/**
	 *  当某个Block缓存的数目超过threshold时，该HashBlock
	 * 会将数据写到Disk.但是同步工作不能在HashBlock使用线程，
	 * 否则就是每个 HashBlock都用了一条线程，可能会有数以百万线程.所以每个Block需要同步时，
	 * 会回调该MyHashMap来使用MyHashMap的线程进行同步
	 */
	@Override
	public void sync(final HashBlock<V> block) {
//		this.threadPool.execute(new Runnable() {
//			
//			@Override
//			public void run() {
//				block.syncData(mapType);
//			}
//		});
		block.syncData(mapType);
	}

	@Override
	public long reAssignBlock(final long oldMetainfo, final int oldBlckNum, final int newBlckNum) {
		final long oldBlckOffset = Utils.getBlockOffset(oldMetainfo);
		final byte blckFileBit = Utils.getBlockFile(oldMetainfo);
		final int oldBlckSize = (int) Utils.getBlockLen(oldMetainfo);
		
		long newBlckOffset = 0;
		long newMetaInfo = 0;
		int newBlckSize = this.initBlockSize * newBlckNum;
		// 先获得新的Block
		String fileName = "";
		if(mapType == Utils.TYPE_ORDER) {
			fileName = fileManager.getOrderBlockFileByBit(blckFileBit);
		}else if(mapType == Utils.TYPE_GOOD) {
			fileName = fileManager.getGoodBlockFileByBit(blckFileBit);
		}else if(mapType == Utils.TYPE_BUYER) {
			fileName = fileManager.getBuyerBlockFileByBit(blckFileBit);
		}else if(mapType == Utils.TYPE_GOOD_ORDER) {
			fileName = fileManager.getGoodOrderBlockFileByBit(blckFileBit);
		}else if(mapType == Utils.TYPE_BUYER_ORDER) {
			fileName = fileManager.getBuyerOrderBlockFileByBit(blckFileBit);
		}
		newBlckOffset = fileManager.getBlckFileMultiOffset(fileName, newBlckNum);
//		MyLogger.info("In reAssignBlock, oldBlckSize=" + oldBlckSize + " oldBlckNum=" + oldBlckNum
//				+ " oldBlckOffset=" + oldBlckOffset + " newBlckSize=" + newBlckSize + " newBlckNum=" + newBlckNum
//				+ " newBlckOffset=" + newBlckOffset + "  this.initBlockSize=" +  this.initBlockSize);
		newMetaInfo = Utils.setBlockFileBit(blckFileBit, newMetaInfo);
		newMetaInfo = Utils.setBlockOffsetBit(newBlckOffset, newMetaInfo);
		newMetaInfo = Utils.setBlockLenBit(newBlckSize, newMetaInfo);
		if(oldBlckNum == 1) {
			// 以前只有一块offset，那也只需还回去一个就好
			fileManager.recyleSingleOffset(fileName, oldBlckOffset);
		} else{
			// 有多个块，需要一起还回去
			ArrayList<Long> oldOffsetList = new ArrayList<Long>();
			long curOffset = oldBlckOffset;
			for(int i = 0; i < oldBlckNum; i++) {
				oldOffsetList.add(curOffset);
				curOffset += oldBlckSize;
			}
			fileManager.recycleMultiOffset(fileName, oldOffsetList);
		}
		
		return newMetaInfo;
	}
	
	/**
	 * 强行同步而且不用线程等待
	 */
	public  void forceSyncAndWait() {
		syncAlready = true;
		for(int i = 0; i < capability; i++) {
			HashBlock<V> block = blocks[i];
			if(block != null) {
				sync(block);
			}else{
//				MyLogger.info("block is null");
			}
		}
	}
	/**
	 * 最后读完文件时应该调用一次，强行同步一次
	 */
	public void forceSync() {
//		this.threadPool.execute(new Runnable() {
//			
//			@Override
//			public void run() {
//				for(int i = 0; i < capability; i++) {
//					
//				}
//			}
//		});
		syncAlready = true;
		for(int i = 0; i < capability; i++) {
			HashBlock<V> block = blocks[i];
			if(block != null) {
				sync(block);
			}else{
	//			MyLogger.info("block is null");
			}
		}
		
	}
	
	/**
	 * 等待构造结束
	 */
	public void waitConstructFinish() {
		this.stop = true;
		this.threadPool.shutdown();
		try {
			boolean flag = this.threadPool.awaitTermination(1L, TimeUnit.HOURS);
//			MyLogger.info("HashMap threadPool terminate:" + flag);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
			MyLogger.info("结束awaitTermination!!!");
		}
//		this.fileManager.close();
//		this.constructFinishListener.constructFinish();
		
	}
	public void close() {
		this.threadPool.shutdown();
		try {
			this.threadPool.awaitTermination(1L, TimeUnit.HOURS);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
			MyLogger.info("结束awaitTermination!!!");
		}
//		for(String key : blockRafMap.keySet()) {
//			try {
//				blockRafMap.get(key).close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
	}
	
	/**
	 * 根据dataMetaInfo去文件读取某一行数据
	 * @param dataMetaInfo
	 * @return
	 */
	private String readRowLineByMetaInfo(final long dataMetaInfo) {
		String line = "";
		byte dataFileBit = Utils.getGBOFile(dataMetaInfo);
		long dataOffset = Utils.getGBOOffset(dataMetaInfo);
		int dataLen = (int) Utils.getGBOLen(dataMetaInfo);
		String dataFilePath = fileManager.getReadFilePathByBit(dataFileBit);
		RandomAccessFile dataRaf = null;
		if(dataRaf == null) {
			try {
				dataRaf = new RandomAccessFile(dataFilePath, "r");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				return null;
			}
		}
		// 开始读这一行数据....
		ByteBuffer dataBuf = null;
		boolean isDirect = false;
		dataBuf = DirectByteBuffPool.takeSmallPool(dataLen);
		if(dataBuf == null) {
			dataBuf = ByteBuffer.allocate(dataLen);
		}else{
			isDirect = true;
		}
		dataBuf.clear();
		dataBuf.limit(dataLen);
		FileChannel dataChannel = dataRaf.getChannel();
		try {
			dataChannel.position(dataOffset);
			dataChannel.read(dataBuf);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		// 终于读到了真正的一行的数据....
		dataBuf.flip();
		byte[] lineBytes = new byte[dataLen];
		dataBuf.get(lineBytes,0, dataLen);
		try {
			line = new String(lineBytes, 0, lineBytes.length, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			if(isDirect) {
				DirectByteBuffPool.putBackSmallPool(dataBuf, dataBuf.capacity());
			}
			return null;
		}
		if(isDirect) {
			DirectByteBuffPool.putBackSmallPool(dataBuf, dataBuf.capacity());
		}
		return line;
	}
}