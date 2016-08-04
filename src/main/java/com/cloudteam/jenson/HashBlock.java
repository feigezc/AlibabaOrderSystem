package com.cloudteam.jenson;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.haiwanwan.common.objectpool.Poolable;


/**
 * HashBlock中的metainfo表示的是索引信息在文件中的位置
 * @author Jenson
 *
 * @param <V> OrderMetaInfo或者GBMetaInfo
 */
public class HashBlock<V>{
//	// 这是专门为了GoodOrderEntry和BuyerOrderEntry设置的
//	// 是goodId或者BuyerId
//	private String id = null;
	public void setId(final String id) {
//		this.id = id;
	}
	public String getId() {
//		return this.id;
		return "";
	}
	/*
	 * 一共64位，从最高位到最低位:
	 * 第63-59: 5位文件路径位。最多32个文件路径
	 * 第58-22: 37位的文件OFFSET位。2^37 Byte = 128GB， 所以单个文件最大不超过128G。
	 * 第21-0: 22位，BlockSize。2^22 Byte = 4MB，所以单个Block所含内容最大不超过8MB。   
	 */
	protected long metainfo = 0;
	/* 
	 * 表示对于该Block，当前已经存了多少数据，也就是在这个块内的下一个可写的位置。
	 * 该位置是相对坐标，相对于该块offset的位置。
	 */
	private int blockCurPos = 0;
	private MyFileWriter myFWriter;
	private MyListener<V> myListener;
	// 最开始分配的块大小
	private int initBlockSize;
	private String filePath;
	protected ConcurrentLinkedQueue<V> elements;
//	private LinkedBlockingQueue<V> elements;
	// 当前有多少个元素 在队列里面，当atomCnt>threshold时进行同步
	protected short threshold;	
	protected AtomicInteger atomCnt;
	
	// 已结写了多少个元素进去了
	private AtomicInteger eleCnt = new AtomicInteger(0);
	/**
	 * 
	 * @param fileBit 该块在哪个文件; 
	 * @param offset  该块的初始位置
	 * @param blockSize 该块的初始大小
	 * @param threshold 当队列缓存的对象超过该阈值时，将队列元素写到文件
	 * @param sync 需要同步文件时的回调对象
	 */
	public HashBlock(final byte fileBit, final long offset, final int blockSize, 
			final MyFileWriter mfw, final MyListener<V> ml, 
			final String path, final short threshold) {
		this.initBlockSize = blockSize;
		this.metainfo = Utils.setBlockFileBit(fileBit, metainfo);
		this.metainfo = Utils.setBlockOffsetBit(offset, metainfo);
		this.metainfo = Utils.setBlockLenBit(blockSize, metainfo);
		this.myFWriter = mfw;
		this.myListener = ml;
		this.filePath = path;
		this.threshold = threshold;
		this.elements = new ConcurrentLinkedQueue<V>();
		this.atomCnt = new AtomicInteger(0);
	}
	
	public long getMetaInfo() {
		return this.metainfo;
	}
	
	/**
	 * 将元数据存进队列。当队列元素>threshold时，将数据写入硬盘
	 * 添加元素时使用的是ConcurrentLinkedQueue，所以是线程安全的；
	 * 当触发sync时，会使用锁
	 * @param e
	 */
	public synchronized void add(V poolableElement, final byte mapType) {
		int byteAData = 0;
		// 一次最多写多少个元素到ByteBuffer
		int maxEleNum = 1;
		// 估算每个索引数据大概的字节，然后估算大概需要多大的ByteBuffer
		if(mapType == Utils.TYPE_ORDER) {
//			MyLogger.info("Order syncData elements size=" + this.elements.size());
			// 订单的索引数据是17byte
			byteAData = 17;
			// 保证不大于2K, 100个是1.66k
			if(maxEleNum > 100) {
				maxEleNum = 100; 
			}
		}else {
//			MyLogger.info("Good or Buyer syncData elements size=" + this.elements.size());
			// 1字节BeginByte + 1 字节Id长度 + 21字节ID + 8字节metainfo 
			// 31 byte
			// 31byte * 10 = 310
			byteAData = 31;
			// 保证不大于2K, 60个是1.8k
			if(maxEleNum > 60) {
				maxEleNum = 60; 
			}
		}
		int oldcnt = this.atomCnt.get();
		this.atomCnt.set(oldcnt - maxEleNum);
		boolean isPool = true;
		int bufSize = maxEleNum * byteAData;
		Poolable<ByteBuffer> poolByteBuf = null;
		ByteBuffer writeBuf = null;
		writeBuf = DirectByteBuffPool.takeSmallPool(bufSize);				
		if(writeBuf == null && ByteBufferPool.sizeFix(bufSize)) {
			poolByteBuf = ByteBufferPool.take(bufSize);
			if(poolByteBuf == null) {
				// 先通知MyFileWrite，再等待
				this.myFWriter.forceSync();
				// 如果大小是合适的，那就不停的Poll
				poolByteBuf = ByteBufferPool.take(bufSize);
			}
			writeBuf = poolByteBuf.getObject();
		}
//		if(writeBuf == null && ByteBufferPool.sizeFix(bufSize)){
//			// 尝试分配一个
//			writeBuf = ByteBufferPool.allocate(bufSize);
//		}
		if(writeBuf == null) {
			isPool = false;
			// 不是合适的大小
			MyLogger.info("ByteBuffer size="+bufSize + " is null");
			writeBuf = ByteBuffer.allocate(bufSize);
		}
//		MyLogger.info("ByteBuffer size="+bufSize +" maxEleNum="+maxEleNum+" cap="+writeBuf.capacity());
		writeBuf.clear();
		long blckoffset = Utils.getBlockOffset(this.metainfo);
		int blckSize = (int) Utils.getBlockLen(this.metainfo);
		// 看看当前Block占用了多少个Block
		int curBlckNum = blckSize / initBlockSize;
		long curPos = this.blockCurPos;
		// writeBuf已经存进的数据长度
		int writeBufLen = 0;
		int eleNum = 0;
			eleNum++;
			Object element = ((Poolable<?>)poolableElement).getObject();
			// 每一个element放一个bytebuffer
			if(element instanceof GBMetaInfo) {
//				MyLogger.info("element is instanceof GBMetaInfo");
				GBMetaInfo gbMetaData = (GBMetaInfo) element;
				// 是Good Buyer的MetaInfo
				byte[] idBytes = gbMetaData.id;
				if(idBytes == null) {
					MyLogger.err("In HashBlock syncMetaData, idBytes is null!");
					return;
				}
				byte idByteLen = gbMetaData.idLen;
				long gbMetaInfo = gbMetaData.metainfo;
				// 1字节BeginByte + 1 字节Id长度 + N字节ID + 8字节metainfo 
				int dataLen = 1 + 1 + idByteLen + 8;	
				int writeBufCurPos = writeBuf.position();
				if(dataLen + curPos + writeBufCurPos > blckSize) {
					MyLogger.info("商品或者买家Block overflow！");
					// 即将写入的dataLen数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
					// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
					long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
					// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
					copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
							newMetaInfo, filePath);
					// 重置信息
					this.metainfo = newMetaInfo;
//					MyLogger.info("重分配之前:curBlckNum="+ curBlckNum + " blckoffset="+ blckoffset
//							+ " blckSize:" + blckSize);
					blckoffset = Utils.getBlockOffset(this.metainfo);
					blckSize = (int) Utils.getBlockLen(this.metainfo);
					curBlckNum = blckSize /  initBlockSize;
//					MyLogger.info("重分配之后:curBlckNum="+ curBlckNum + " blckoffset="+ blckoffset
//							+ " blckSize:" + blckSize);
				}
				// 这两种数据在硬盘中的布局为:
				// 1字节BeginByte + 1 字节Id长度 + N字节ID + 8字节metainfo					
				writeBuf.put(GBMetaInfo.beginByte);
				writeBuf.put(idByteLen);
				writeBuf.put(idBytes, 0, idByteLen);
				writeBuf.putLong(gbMetaInfo);
				writeBufLen += dataLen;
				GBMetaInfoPool.put(poolableElement);
				gbMetaData = null;
			}else if(element instanceof OrderMetaInfo){
//				MyLogger.info("element is instanceof OrderMetaInfo");
				OrderMetaInfo orderMetaData = (OrderMetaInfo) element;
				// 1 byte BeginByte + 8 byte orderId + 8 byte metaInfo
				long orderId = orderMetaData.orderId;
				long metainfo = orderMetaData.metaInfo;
				int dataLen = 1 + 8 + 8;					
				
				int writeBufCurPos = writeBuf.position();
				if(dataLen + curPos + writeBufCurPos > blckSize) {
					MyLogger.info("订单Block overflow！");
					// 即将写入的dataLen Byte数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
					// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
					// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
					long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
					copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
							newMetaInfo, filePath);
					// 重置信息
					this.metainfo = newMetaInfo;
					blckoffset = Utils.getBlockOffset(this.metainfo);
					blckSize = (int) Utils.getBlockLen(this.metainfo);
					curBlckNum = blckSize / initBlockSize;
				}
				// 1 byte BeginByte + 8 byte orderId + 8 byte metaInfo
				writeBuf.put(OrderMetaInfo.beginByte);
				writeBuf.putLong(orderId);
				writeBuf.putLong(metainfo);
				
				writeBufLen += dataLen;
				OrderMetaInfoPool.put(poolableElement);;
				orderMetaData = null;
			}else if(element instanceof BuyerGoodOrderEntry){
//				MyLogger.info("BuyerGoodOrderBlock has written:" + eleCnt.incrementAndGet());
//				MyLogger.info("同步BuyerOrderEntry数据");
				/*
				 * 该entry在文件中的布局应为:
				 * 1byt表示数据开始+ 1byte表示买家/商品ID的长度 + 不定长的买家/商品id + 8byte的订单ID.
				 */
				BuyerGoodOrderEntry buyerOrderData = (BuyerGoodOrderEntry) element;
				byte idByteLen = buyerOrderData.idLen;
				long orderId = buyerOrderData.orderId;
				int dataLen = 1 + 1 + idByteLen + 8;									
				int writeBufCurPos = writeBuf.position();
				if(dataLen + curPos + writeBufCurPos > blckSize) {						
					MyLogger.info("BuyerGoodOrderEntryBlock overflow！blckSize=" + blckSize);
					// 即将写入的dataLen Byte数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
					// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
					// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
					long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
					copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
							newMetaInfo, filePath);
					// 重置信息
					this.metainfo = newMetaInfo;
					blckoffset = Utils.getBlockOffset(this.metainfo);
					blckSize = (int) Utils.getBlockLen(this.metainfo);
					curBlckNum = blckSize / initBlockSize;
				}
				writeBuf.put(BuyerGoodOrderEntry.beginByte);
				writeBuf.put(idByteLen);
				writeBuf.put(buyerOrderData.id, 0, idByteLen);
				writeBuf.putLong(orderId);
				
				writeBufLen += dataLen;
				BuyerGoodOrderEntryPool.put(poolableElement);;
				buyerOrderData = null;
			}
			
		// 写进MyFileWriter
		writeBuf.flip();
		int bufDataLen = writeBuf.remaining();
		FileData fd = new FileData(curPos+blckoffset, poolByteBuf,
				bufDataLen, isPool);
		this.myFWriter.writeData(fd);
		this.blockCurPos += bufDataLen;
	}

	/**
	 * 将元数据存进队列。当队列元素>threshold时，将数据写入硬盘
	 * 添加元素时使用的是ConcurrentLinkedQueue，所以是线程安全的；
	 * 当触发sync时，会使用锁
	 * @param e
	 */
	/**
	 * 一有元素进来就写到FileWriter
	 * @param e
	 */
	public void syncData(final byte mapType) {
		synchronized (this) {
			if(this.elements.size() <= 0) {
				return;
			}
			
			int byteAData = 0;
			// 一次最多写多少个元素到ByteBuffer
			int maxEleNum = this.elements.size();
			// 估算每个索引数据大概的字节，然后估算大概需要多大的ByteBuffer
			if(mapType == Utils.TYPE_ORDER) {
//				MyLogger.info("Order syncData elements size=" + this.elements.size());
				// 订单的索引数据是17byte
				byteAData = 17;
				// 保证不大于2K, 100个是1.66k
				if(maxEleNum > 100) {
					maxEleNum = 100; 
				}
			}else {
//				MyLogger.info("Good or Buyer syncData elements size=" + this.elements.size());
				// 1字节BeginByte + 1 字节Id长度 + 21字节ID + 8字节metainfo 
				// 31 byte
				// 31byte * 10 = 310
				byteAData = 31;
				// 保证不大于2K, 60个是1.8k
				if(maxEleNum > 60) {
					maxEleNum = 60; 
				}
			}
			int oldcnt = this.atomCnt.get();
			this.atomCnt.set(oldcnt - maxEleNum);
			boolean isPool = true;
			int bufSize = maxEleNum * byteAData;
			Poolable<ByteBuffer> poolByteBuf = null;
			ByteBuffer writeBuf = null;
			writeBuf = DirectByteBuffPool.takeSmallPool(bufSize);				
			if(writeBuf == null && ByteBufferPool.sizeFix(bufSize)) {
				poolByteBuf = ByteBufferPool.take(bufSize);
				if(poolByteBuf == null) {
					// 先通知MyFileWrite，再等待
					this.myFWriter.forceSync();
					// 如果大小是合适的，那就不停的Poll
					poolByteBuf = ByteBufferPool.take(bufSize);
				}
				writeBuf = poolByteBuf.getObject();
			}
//			if(writeBuf == null && ByteBufferPool.sizeFix(bufSize)){
//				// 尝试分配一个
//				writeBuf = ByteBufferPool.allocate(bufSize);
//			}
			if(writeBuf == null) {
				isPool = false;
				// 不是合适的大小
				MyLogger.info("ByteBuffer size="+bufSize + " is null");
				writeBuf = ByteBuffer.allocate(bufSize);
			}
//			MyLogger.info("ByteBuffer size="+bufSize +" maxEleNum="+maxEleNum+" cap="+writeBuf.capacity());
			writeBuf.clear();
			long blckoffset = Utils.getBlockOffset(this.metainfo);
			int blckSize = (int) Utils.getBlockLen(this.metainfo);
			// 看看当前Block占用了多少个Block
			int curBlckNum = blckSize / initBlockSize;
			long curPos = this.blockCurPos;
			// writeBuf已经存进的数据长度
			int writeBufLen = 0;
			V poolableElement = null;
			int eleNum = 0;
			while((poolableElement = elements.poll()) != null) {				
				eleNum++;
				Object element = ((Poolable<?>)poolableElement).getObject();
				// 每一个element放一个bytebuffer
				if(element instanceof GBMetaInfo) {
//					MyLogger.info("element is instanceof GBMetaInfo");
					GBMetaInfo gbMetaData = (GBMetaInfo) element;
					// 是Good Buyer的MetaInfo
					byte[] idBytes = gbMetaData.id;
					if(idBytes == null) {
						MyLogger.err("In HashBlock syncMetaData, idBytes is null!");
						return;
					}
					byte idByteLen = gbMetaData.idLen;
					long gbMetaInfo = gbMetaData.metainfo;
					// 1字节BeginByte + 1 字节Id长度 + N字节ID + 8字节metainfo 
					int dataLen = 1 + 1 + idByteLen + 8;	
					int writeBufCurPos = writeBuf.position();
					if(dataLen + curPos + writeBufCurPos > blckSize) {
						MyLogger.info("商品或者买家Block overflow！");
						// 即将写入的dataLen数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
						// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
						long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
						// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
						copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
								newMetaInfo, filePath);
						// 重置信息
						this.metainfo = newMetaInfo;
	//					MyLogger.info("重分配之前:curBlckNum="+ curBlckNum + " blckoffset="+ blckoffset
	//							+ " blckSize:" + blckSize);
						blckoffset = Utils.getBlockOffset(this.metainfo);
						blckSize = (int) Utils.getBlockLen(this.metainfo);
						curBlckNum = blckSize /  initBlockSize;
	//					MyLogger.info("重分配之后:curBlckNum="+ curBlckNum + " blckoffset="+ blckoffset
	//							+ " blckSize:" + blckSize);
					}
					// 这两种数据在硬盘中的布局为:
					// 1字节BeginByte + 1 字节Id长度 + N字节ID + 8字节metainfo					
					writeBuf.put(GBMetaInfo.beginByte);
					writeBuf.put(idByteLen);
					writeBuf.put(idBytes, 0, idByteLen);
					writeBuf.putLong(gbMetaInfo);
					writeBufLen += dataLen;
					GBMetaInfoPool.put(poolableElement);
					gbMetaData = null;
				}else if(element instanceof OrderMetaInfo){
//					MyLogger.info("element is instanceof OrderMetaInfo");
					OrderMetaInfo orderMetaData = (OrderMetaInfo) element;
					// 1 byte BeginByte + 8 byte orderId + 8 byte metaInfo
					long orderId = orderMetaData.orderId;
					long metainfo = orderMetaData.metaInfo;
					int dataLen = 1 + 8 + 8;					
					
					int writeBufCurPos = writeBuf.position();
					if(dataLen + curPos + writeBufCurPos > blckSize) {
						MyLogger.info("订单Block overflow！");
						// 即将写入的dataLen Byte数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
						// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
						// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
						long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
						copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
								newMetaInfo, filePath);
						// 重置信息
						this.metainfo = newMetaInfo;
						blckoffset = Utils.getBlockOffset(this.metainfo);
						blckSize = (int) Utils.getBlockLen(this.metainfo);
						curBlckNum = blckSize / initBlockSize;
					}
					// 1 byte BeginByte + 8 byte orderId + 8 byte metaInfo
					writeBuf.put(OrderMetaInfo.beginByte);
					writeBuf.putLong(orderId);
					writeBuf.putLong(metainfo);
					
					writeBufLen += dataLen;
					OrderMetaInfoPool.put(poolableElement);;
					orderMetaData = null;
				}else if(element instanceof BuyerGoodOrderEntry){
//					MyLogger.info("BuyerGoodOrderBlock has written:" + eleCnt.incrementAndGet());
	//				MyLogger.info("同步BuyerOrderEntry数据");
					/*
					 * 该entry在文件中的布局应为:
					 * 1byt表示数据开始+ 1byte表示买家/商品ID的长度 + 不定长的买家/商品id + 8byte的订单ID.
					 */
					BuyerGoodOrderEntry buyerOrderData = (BuyerGoodOrderEntry) element;
					byte idByteLen = buyerOrderData.idLen;
					long orderId = buyerOrderData.orderId;
					int dataLen = 1 + 1 + idByteLen + 8;									
					int writeBufCurPos = writeBuf.position();
					if(dataLen + curPos + writeBufCurPos > blckSize) {						
						MyLogger.info("BuyerGoodOrderEntryBlock overflow！blckSize=" + blckSize);
						// 即将写入的dataLen Byte数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
						// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
						// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
						long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
						copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
								newMetaInfo, filePath);
						// 重置信息
						this.metainfo = newMetaInfo;
						blckoffset = Utils.getBlockOffset(this.metainfo);
						blckSize = (int) Utils.getBlockLen(this.metainfo);
						curBlckNum = blckSize / initBlockSize;
					}
					writeBuf.put(BuyerGoodOrderEntry.beginByte);
					writeBuf.put(idByteLen);
					writeBuf.put(buyerOrderData.id, 0, idByteLen);
					writeBuf.putLong(orderId);
					
					writeBufLen += dataLen;
					BuyerGoodOrderEntryPool.put(poolableElement);;
					buyerOrderData = null;
				}
				if(eleNum >= maxEleNum - 4){
					break;
				}
			}
			// 写进MyFileWriter
			writeBuf.flip();
			int bufDataLen = writeBuf.remaining();
			FileData fd = new FileData(curPos+blckoffset, poolByteBuf,
					bufDataLen, isPool);
			this.myFWriter.writeData(fd);
			this.blockCurPos += bufDataLen;
			
		} // end synchronized
	}
	
	/**
	 * 将旧的块上的数据复制到新的块
	 * @param oldMetaInfo 旧的块的元数据
	 * @param len		旧的块的数据长度
	 * @param newMetaInfo	新的块
	 */
	protected void copyBlckToNewBlck(final long oldMetaInfo, final int len,
			final long newMetaInfo, final String path) {
		synchronized (this) {
			RandomAccessFile raf = null;
			try {
				raf = new RandomAccessFile(path, "rw");
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			if(raf == null) {
				return;
			}
			FileChannel fileChannel = raf.getChannel();
			long oldBlkOffset = Utils.getBlockOffset(oldMetaInfo);
			int oldBlkCurPos = this.blockCurPos;
			long newBlkOffset = Utils.getBlockOffset(newMetaInfo);
			try {
				// 从旧位置读数据
				fileChannel.position(oldBlkOffset);
				ByteBuffer dataBuffer = ByteBuffer.allocate(oldBlkCurPos);
				fileChannel.read(dataBuffer);
				// 将数据写到新的位置
				dataBuffer.flip();
				fileChannel.position(newBlkOffset);
				while(dataBuffer.hasRemaining()) {
					fileChannel.write(dataBuffer);
				}
//				fileChannel.force(true);
//				fileChannel.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
//			MyLogger.info("copyBlckToNewBlck");
		}
	}
	
//	/**
//	 * 将该Block的队列里的数据写到文件里
//	 * 同步时需要上锁
//	 */
//	public void syncData(final String filePath, final int initBlockSize) {
//		synchronized (this) {
//			long blckoffset = Utils.getBlockOffset(this.metainfo);
//			int blckSize = (int) Utils.getBlockLen(this.metainfo);
//			// 看看当前Block占用了多少个Block
//			int curBlckNum = blckSize / initBlockSize;
//			long curPos = this.blockCurPos;
//			ByteBuffer writeBuf = null;
//			boolean isPool = true;
//			writeBuf = DirectByteBuffPool.take(blckSize);
//			if(writeBuf == null) {
////				isDirect = false;
//				writeBuf = ByteBufferPool.take(blckSize);
//			}
//			if(writeBuf == null) {
//				isPool = false;
//				writeBuf = ByteBuffer.allocate(blckSize);
//			}
//			writeBuf.clear();
//			// writeBuf已经存进的数据长度
//			int writeBufLen = 0;
//			V element = null;
//			boolean queueEmpty = true;
//			while((element = elements.poll()) != null) {
//				queueEmpty = false;
//				if(element instanceof GBMetaInfo) {
//					GBMetaInfo gbMetaData = (GBMetaInfo) element;
//					// 是Good Buyer的MetaInfo
//					// 这两种数据在硬盘中的布局为:
//					// 1byte表示数据的开始+2位的short表示接下来的ID字符串字节数组的长度+不定长的Id的字符串的字节数据  + 
//					// 4位的int表示接下来的不定长的数据的长 + 不定长的字节数据
//					String id = gbMetaData.id;					
//					byte[] idBytes = null;
//					try {
//						idBytes = id.getBytes("UTF-8");
//					} catch (UnsupportedEncodingException e) {
//						e.printStackTrace();
//					}
//					if(idBytes == null) {
//						MyLogger.err("In HashBlock syncMetaData, idBytes is null!");
//						return;
//					}
//					short idByteLen = (short) idBytes.length;
//					byte[] dataBytes = gbMetaData.data;					
//					int dataBytesLen =  dataBytes.length;
//					
//					int dataLen = 1 + 2 + idByteLen + 4 + dataBytesLen;		
//					int writeBufCurPos = writeBuf.position();
//					if(dataLen + curPos + writeBufCurPos > blckSize) {
//						MyLogger.info("商品或者买家Block overflow！");
//						// 即将写入的dataLen数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
//						// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
//						long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
//						// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
//						copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
//								newMetaInfo, filePath);
//						// 重置信息
//						this.metainfo = newMetaInfo;
////						MyLogger.info("重分配之前:curBlckNum="+ curBlckNum + " blckoffset="+ blckoffset
////								+ " blckSize:" + blckSize);
//						blckoffset = Utils.getBlockOffset(this.metainfo);
//						blckSize = (int) Utils.getBlockLen(this.metainfo);
//						curBlckNum = blckSize /  initBlockSize;
////						MyLogger.info("重分配之后:curBlckNum="+ curBlckNum + " blckoffset="+ blckoffset
////								+ " blckSize:" + blckSize);
//					}
//					writeBuf.put(GBMetaInfo.beginByte);
//					writeBuf.putShort(idByteLen);
//					writeBuf.put(idBytes);
//					writeBuf.putInt(dataBytesLen);
//					writeBuf.put(dataBytes);
//					
//					writeBufLen += dataLen;
//				}else if(element instanceof OrderMetaInfo){
//					OrderMetaInfo orderMetaData = (OrderMetaInfo) element;
//					// 是订单的Metainfo
//					// 订单元数据在硬盘中的布局为:
//					// 1byte表示数据的开始+ 8Byte的Long表示orderId 
//					//  + 4Byte的int表示接下来的不定长数据的长度
//					//  +不定长的订单字节数据
//					byte[] orderDataBytes = orderMetaData.data;
//					int orderDataLen =  orderDataBytes.length;
//					long orderId = orderMetaData.orderId;
//					int dataLen = 1 + 8 + 4 + orderDataLen;
//					int writeBufCurPos = writeBuf.position();
//					if(dataLen + curPos + writeBufCurPos > blckSize) {
//						MyLogger.info("订单Block overflow！");
//						// 即将写入的dataLen Byte数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
//						// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
//						// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
//						long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
//						copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
//								newMetaInfo, filePath);
//						// 重置信息
//						this.metainfo = newMetaInfo;
//						blckoffset = Utils.getBlockOffset(this.metainfo);
//						blckSize = (int) Utils.getBlockLen(this.metainfo);
//						curBlckNum = blckSize / initBlockSize;
//					}
//					writeBuf.put(OrderMetaInfo.beginByte);
//					writeBuf.putLong(orderId);
//					writeBuf.putInt(orderDataLen);
//					writeBuf.put(orderDataBytes);
//					
//					writeBufLen += dataLen;
//				}else if(element instanceof BuyerOrderEntry){
////					MyLogger.info("同步BuyerOrderEntry数据");
//					/*
//					 * 该entry在文件中的布局应为:
//					 * 1byte表示数据开始 + 4byte Len + 不定长的orderData.
//					 * 其中Len表示orderData的长度
//					 */
//					BuyerOrderEntry buyerOrderData = (BuyerOrderEntry) element;
//					byte[] boDataBytes = buyerOrderData.orderData;
//					int boDataLen =  boDataBytes.length;
////					long orderId = buyerOrderData.orderId;
//					int dataLen = 1 + 4 + boDataLen;
//					int writeBufCurPos = writeBuf.position();
//					if(dataLen + curPos + writeBufCurPos > blckSize) {
//						MyLogger.info("BuyerOrderBlock overflow！");
//						// 即将写入的dataLen Byte数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
//						// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
//						// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
//						long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
//						copyBlckToNewBlck(this.metainfo, this.blockCurPos, 
//								newMetaInfo, filePath);
//						// 重置信息
//						this.metainfo = newMetaInfo;
//						blckoffset = Utils.getBlockOffset(this.metainfo);
//						blckSize = (int) Utils.getBlockLen(this.metainfo);
//						curBlckNum = blckSize / initBlockSize;
//					}
//					writeBuf.put(BuyerOrderEntry.beginByte);
//					writeBuf.putInt(boDataLen);
//					writeBuf.put(boDataBytes);
//					
//					writeBufLen += dataLen;
//				}else if(element instanceof GoodOrderEntry){
////					MyLogger.info("同步GoodOrderEntry数据");
//					/*
//					 * 该entry在文件中的布局应为:
//					 * 1byte表示数据的开始+8byte orderId + 4byte Len + 不定长的orderData.
//					 * 其中Len表示orderData的长度
//					 */
//					GoodOrderEntry goodOrderData = (GoodOrderEntry) element;
//					byte[] goDataBytes = goodOrderData.orderData;
//					int goDataLen = goDataBytes.length;
////					long orderId = goodOrderData.orderId;
//					int dataLen = 1 + 4 + goDataLen;
//					int writeBufCurPos = writeBuf.position();
//					if(dataLen + curPos + writeBufCurPos > blckSize) {
//						MyLogger.info("GoodOrderEntryBlock overflow！");
//						// 即将写入的dataLen Byte数据+该Block已经写入的数据(blockCurPos)+当前buffer已经写入的数据
//						// 如果已经超过了一个块的大小，需要重新分配.每次重新分配都double块大小。
//						// 重新分配块以后，需要把旧的块的数据全拷贝到新的块
//						long newMetaInfo = this.myListener.reAssignBlock(this.metainfo, curBlckNum, curBlckNum<<1);
//						copyBlckToNewBlck(this.metainfo, this.blockCurPos,
//								newMetaInfo, filePath);
//						// 重置信息
//						this.metainfo = newMetaInfo;
//						blckoffset = Utils.getBlockOffset(this.metainfo);
//						blckSize = (int) Utils.getBlockLen(this.metainfo);
//						curBlckNum = blckSize / initBlockSize;
//					}
//					writeBuf.put(GoodOrderEntry.beginByte);
//					writeBuf.putInt(goDataLen);
//					writeBuf.put(goDataBytes);
//					
//					writeBufLen += dataLen;
//				}
//			}
//			if(queueEmpty){
//				return;
//			}
//			writeBuf.flip();
//			int bufDataLen = writeBuf.remaining();
//			FileData fd = new FileData(curPos+blckoffset, writeBuf, blckSize, isPool);
//			this.myFWriter.writeData(fd);
//			this.blockCurPos += bufDataLen;
//		
//		} // end synchronized
//	}
	
	
}