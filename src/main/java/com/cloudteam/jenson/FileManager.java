package com.cloudteam.jenson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 某个文件的数据，比如该文件的下一个可用的offset.
 * 每个文件对应一个
 * @author Jenson
 *
 */
class FileMngData{
	/**
	 * 使用一个容量只有1000的阻塞队列来维护下一个可用的offset。对于某个文件，当初始化HashBlock，
	 * 需要给该Block赋一个初始Offset时，会到该队列取Offset。
	 * 该队列的offset保证连续
	 */
	private LinkedBlockingQueue<Long> offsetQueue = new LinkedBlockingQueue<Long>(500);
	/**
	 * 使用一个并发队列来存HashBlock回收过来的offset.
	 * 当一个HashBlock里面存的数据超过了初始设置的块大小时，该Block需要重新分配Offset并修改Block大小，
	 * 这时该Block之前所占的块的Offset便需要“回收”.该队列的offset不保证连续
	 */
	private ConcurrentLinkedQueue<Long> recycleOffset = new ConcurrentLinkedQueue<Long>();
	/*
	 *  由于可能需要一次读两个相邻的offset，所以在写offsetQueue时和一次读1个以上
	 *  的offset时都需要锁，否则可能出现一次读两个offset却不相邻的现象
	 */
	private Lock offsetLock = new ReentrantLock(false);
	
	private ExecutorService threadPool;;
	private boolean exits = false;
	private long blockSize;
	public FileMngData(long size) {
		this.threadPool = Executors.newFixedThreadPool(1);
		this.blockSize = size;
		this.threadPool.submit(new Runnable() {
			@Override
			public void run() {
				long curOffset = 0;
				while(true) {
					if(exits) {
						break;
					}
					// 产生新的Offset;产生新的offset无需锁，因为是在队列的末尾写入，
					// 而读取offset是在队列头，不会影响一次需要读多个offset的情况
					// TODO:理论上，应该不会出现：新产生的offset与recycleOffset回收的offset相同的现象					
					try {
						offsetQueue.put(curOffset);
//						MyLogger.info("FileMngData put offset:" + curOffset);
					} catch (InterruptedException e) {
//						e.printStackTrace();
						exits = true;
						return;
					}
					curOffset += blockSize;
				}
			}
		});
	}
	
	public void close() {
		this.recycleOffset.clear();
		this.offsetQueue.clear();
		this.threadPool.shutdownNow();
	}
	/**
	 * 获得下一个可用的offset
	 * @return -1如果发生异常
	 */
	public long getOffset() {
		// 首先看recycleOffset是否有offset，如果有
		// 则先用recycleOffset的
		Long offset = recycleOffset.poll();
		if(offset != null) {
			return offset;
		}
		
		try {
			 offset =  offsetQueue.take();
			return offset;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	/**
	 * 获得多个可用的offset，相当于需要分配多个Block
	 * @param num 需要分配的Block数
	 * @return 如果成功，返回第一个Offset。发生异常则返回-1
	 */
	public long getNumOffset(int num) {
		// 获得多个Block Offset需要上锁		
		Long firstOffset = null;
		try{
			// 只有在需要一次读多个offset时才需要上锁
			offsetLock.lock();
			for(int i = 0; i < num; i++) {
				if(i == 0) {
					firstOffset = offsetQueue.take();
				} else{
					offsetQueue.take();
				}
			}
		}catch(InterruptedException ex) {
			ex.printStackTrace();
			firstOffset = -1L;
		}finally{
			offsetLock.unlock();
		}
		return firstOffset;
	}
	
	/**
	 * 回收单个offset
	 * @param offset
	 */
	public void recyleSingleOffset(Long offset) {
		recycleOffset.offer(offset);
	}
	
	/**
	 * 回收多个offset
	 * @param offsetList
	 */
	public void recycleMultiOffset(ArrayList<Long> offsetList) {
		for(Long offset: offsetList) {
			recycleOffset.offer(offset);
		}
	}
}
/**
 * 管理与文件相关的数据，比如某个文件下一个可用的offset
 * @author Jenson
 *
 */
public class FileManager {
	
	// 这些表示官方给的输入文件的路径
	private ArrayList<String> orderFilesPath;
	private ArrayList<String> buyerFilesPath;
	private ArrayList<String> goodFilesPath;
	/* 输入数据文件路径的Byte表示， 包括Order,Buyer,Good三种文件 */
	// key = filepath, value = file bit
	private HashMap<String, Byte> readFilePathBit;
	private HashMap<Byte, String> readFileBitPath;
	
	private ArrayList<String> storeFoldersPath;
	
	
	// 用来存订单Block文件的路径
	private ArrayList<String> orderBlockFilePath;
	// 用来存商品Block文件的路径
	private ArrayList<String> goodBlockFilePath;
	// 用来存商品,订单联合Block文件的路径
	private ArrayList<String> goodOrderBlockFilePath;
	// 用来存买家Block文件的路径
	private ArrayList<String> buyerBlockFilePath;
	// 用来存买家,订单联合Block文件的路径
	private ArrayList<String> buyerOrderBlockFilePath;
	
	private HashMap<String, FileMngData> blckFileOffsetMap = new HashMap<String, FileMngData>();
	
//	// 初始Block大小  （不能用统一的大小了）
//	private int initialBlockSize;
	// 每种hashmap对应一个blockSize
	private int orderMapBlkSize = 0;
	private int goodMapBlkSize = 0;
	private int buyerMapBlkSize = 0;
	private int buyerOrderMapBlkSize = 0;
	private int goodOrderMapBlkSize = 0;
	
	public FileManager(Collection<String> orderFiles,
			Collection<String> buyerFiles, Collection<String> goodFiles,
			Collection<String> storeFolders) {
		this.orderFilesPath = new ArrayList<String>();
		this.buyerFilesPath = new ArrayList<String>();
		this.goodFilesPath = new ArrayList<String>();
		
		this.storeFoldersPath = new ArrayList<String>();
		
		this.orderFilesPath.addAll(orderFiles);
		this.buyerFilesPath.addAll(buyerFiles);
		this.goodFilesPath.addAll(goodFiles);
		this.storeFoldersPath.addAll(storeFolders);
		
		this.readFilePathBit = new HashMap<String, Byte>();
		this.readFileBitPath = new HashMap<Byte, String>();
		
		byte idx = 0;
		for(String path : orderFilesPath) {
			readFilePathBit.put(path, idx);
			readFileBitPath.put(idx, path);
//			MyLogger.info("输入文件path: " + path + " fileBit:" + idx);
			idx++;
		}
		for(String path : buyerFilesPath) {
			readFilePathBit.put(path, idx);
			readFileBitPath.put(idx, path);
//			MyLogger.info("输入文件path: " + path + " fileBit:" + idx);
			idx++;
		}
		for(String path : goodFilesPath) {
			readFilePathBit.put(path, idx);
			readFileBitPath.put(idx, path);
//			MyLogger.info("输入文件path: " + path + " fileBit:" + idx);
			idx++;
		}

		this.orderBlockFilePath = new ArrayList<String>();
		this.goodBlockFilePath = new ArrayList<String>();
		this.goodOrderBlockFilePath = new ArrayList<String>();
		this.buyerBlockFilePath = new ArrayList<String>();
		this.buyerOrderBlockFilePath = new ArrayList<String>();		
	}
	
	public void initData() {
		//Order  Good  Buyer索引文件都只建一个
		int idx = 0;
		for(String path : this.storeFoldersPath){
			if(path.charAt(Utils.storeDiskTypePos) == '1') {
				
			}else if(path.charAt(Utils.storeDiskTypePos) == '2'){
				// Good存disk2
				String goodFileName = path + "goodblock.dat";
				this.goodBlockFilePath.add(goodFileName);
				this.blckFileOffsetMap.put(goodFileName, new FileMngData(this.goodMapBlkSize));
				
//				// 订单存disk2
//				String orderFileName = path + "orderblock.dat2";
//				this.orderBlockFilePath.add(orderFileName);
//				this.blckFileOffsetMap.put(orderFileName, new FileMngData(this.orderMapBlkSize));
			}
			else if(path.charAt(Utils.storeDiskTypePos) == '3') {
				// 订单存disk3
				String orderFileName = path + "orderblock.dat3";
				this.orderBlockFilePath.add(orderFileName);
				this.blckFileOffsetMap.put(orderFileName, new FileMngData(this.orderMapBlkSize));
				
				// BuyerOrder和GoodOrder都和订单一样存Disk3
				String buyerOrderFileName = path + "buyerorderblock.dat";
				this.buyerOrderBlockFilePath.add(buyerOrderFileName);
				this.blckFileOffsetMap.put(buyerOrderFileName, new FileMngData(this.buyerOrderMapBlkSize));
				
				String goodOrderFileName = path + "goodorderblock.dat";
				this.goodOrderBlockFilePath.add(goodOrderFileName);
				this.blckFileOffsetMap.put(goodOrderFileName, new FileMngData(this.goodOrderMapBlkSize));
				
				// 买家存disk3
				String buyerFileName = path + "buyerblock.dat";
				this.buyerBlockFilePath.add(buyerFileName);
				this.blckFileOffsetMap.put(buyerFileName, new FileMngData(this.buyerMapBlkSize));
			}
		}

	}
	public void setOrderMapBlkSize(int orderMapBlkSize) {
		this.orderMapBlkSize = orderMapBlkSize;
	}

	public void setGoodMapBlkSize(int goodMapBlkSize) {
		this.goodMapBlkSize = goodMapBlkSize;
	}

	public void setBuyerMapBlkSize(int buyerMapBlkSize) {
		this.buyerMapBlkSize = buyerMapBlkSize;
	}

	public void setBuyerOrderMapBlkSize(int buyerOrderMapBlkSize) {
		this.buyerOrderMapBlkSize = buyerOrderMapBlkSize;
	}

	public void setGoodOrderMapBlkSize(int goodOrderMapBlkSize) {
		this.goodOrderMapBlkSize = goodOrderMapBlkSize;
	}



	public final ArrayList<String> getOrderBlockFilePath() {
		return this.orderBlockFilePath;
	}
	
	public final ArrayList<String> getGoodBlockFilePath() {
		return this.goodBlockFilePath;
	}
	public final ArrayList<String> getGoodOrderBlockFilePath() {
		return this.goodOrderBlockFilePath;
	}
	
	public final ArrayList<String> getBuyerBlockFilePath() {
		return this.buyerBlockFilePath;
	}
	public final ArrayList<String> getBuyerOrderBlockFilePath() {
		return this.buyerOrderBlockFilePath;
	}
	/**
	 * 获得特定Block文件的下一个可用的Offset.如果offset用完了，可能会发生阻塞
	 * @param file
	 * @return
	 */
	public long getBlckFileOneOffset(String file) {
		return this.blckFileOffsetMap.get(file).getOffset();
	}
	
	/**
	 * 获得特定Block文件的多个可用的Offset.如果offset用完了，可能会发生阻塞
	 * @param file
	 * @param num 需要分配的Block数目
	 * @return
	 */
	public long getBlckFileMultiOffset(String file, int num) {
		return this.blckFileOffsetMap.get(file).getNumOffset(num);
	}
	
	/**
	 * 回收单个offset
	 * @param offset
	 */
	public void recyleSingleOffset(String file, Long offset) {
		this.blckFileOffsetMap.get(file).recyleSingleOffset(offset);
	}
	
	/**
	 * 回收多个offset
	 * @param offsetList
	 */
	public void recycleMultiOffset(String file, ArrayList<Long> offsetList) {
		this.blckFileOffsetMap.get(file).recycleMultiOffset(offsetList);
	}
	
	/**
	 * 根据序号来分配OrderBlock到OrderBlock文件
	 * @param idx Block的序号
	 * @return 分配给该Block的序号
	 */
	public byte getOrderBlockFileBit(int idx) {
		// 用Mod平均分配Block到文件
		return (byte) (idx % this.orderBlockFilePath.size());
	}
	
	public byte getGoodBlockFileBit(int idx) {
		// 用Mod平均分配Block到文件
		return (byte) (idx % this.goodBlockFilePath.size());
	}
	public byte getGoodOrderBlockFileBit(int idx) {
		// 用Mod平均分配Block到文件
		return (byte) (idx % this.goodOrderBlockFilePath.size());
	}
	
	public byte getBuyerBlockFileBit(int idx) {
		// 用Mod平均分配Block到文件
		return (byte) (idx % this.buyerBlockFilePath.size());
	}
	
	public byte getBuyerOrderBlockFileBit(int idx) {
		// 用Mod平均分配Block到文件
		return (byte) (idx % this.buyerOrderBlockFilePath.size());
	}
	
	/**
	 * 根据文件位来获得对应的OrderBlock文件的路径
	 * @param filebit
	 * @return
	 */
	public String getOrderBlockFileByBit(byte filebit) {
		int idx = filebit;
		if(idx > this.orderBlockFilePath.size())
			return null;
		return this.orderBlockFilePath.get(idx);
	}
	
	public String getGoodBlockFileByBit(byte filebit) {
		int idx = filebit;
		if(idx > this.goodBlockFilePath.size())
			return null;
		return this.goodBlockFilePath.get(idx);
	}
	public String getGoodOrderBlockFileByBit(byte filebit) {
		int idx = filebit;
		if(idx > this.goodOrderBlockFilePath.size())
			return null;
		return this.goodOrderBlockFilePath.get(idx);
	}
	
	public String getBuyerBlockFileByBit(byte filebit) {
		int idx = filebit;
		if(idx > this.buyerBlockFilePath.size())
			return null;
		return this.buyerBlockFilePath.get(idx);
	}
	public String getBuyerOrderBlockFileByBit(byte filebit) {
		int idx = filebit;
		if(idx > this.buyerOrderBlockFilePath.size())
			return null;
		return this.buyerOrderBlockFilePath.get(idx);
	}
	
	
	/**
	 * 根据输入数据文件存储路径找到对应的Bit
	 * @param path 文件路径
	 * @return
	 */
	public byte getReadFileBitByPath(String path) {
		return this.readFilePathBit.get(path);
	}
	
	/**
	 * 根据输入文件bit位来获取输入文件路径
	 * @param fileBit
	 * @return
	 */
	public String getReadFilePathByBit(byte fileBit) {
		return this.readFileBitPath.get(fileBit);
	}
	public ArrayList<String> getOrderFilesPath() {
		return orderFilesPath;
	}

	public ArrayList<String> getBuyerFilesPath() {
		return buyerFilesPath;
	}

	public ArrayList<String> getGoodFilesPath() {
		return goodFilesPath;
	}

	public ArrayList<String> getStoreFoldersPath() {
		return storeFoldersPath;
	}
	
	public void close() {
		for(String key : blckFileOffsetMap.keySet()) {
			blckFileOffsetMap.get(key).close();
		}
	}

}
