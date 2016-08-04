package com.cloudteam.jenson;


import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.cloudteam.xiaode.Row;

/**
 * 使用Java自带的 LinkedHashMap实现LRUCache.
 * 对同一种cache的读写需要上锁
 * 还是不用锁吧，这样查询就相当于串行了，就算数据不一致，也就是多一次IO而已
 * @author Jenson
 *
 */
public class SimpleLRUCache {
	private static final float LOAD_FAC = 0.75F;
	// 按照访问顺序排序
	private static final boolean ACC_ORDER = true;
	// 最少缓存10000条L1cache
	private static final int MIN_L1CACHE_NUM = 10000;		
	// 最少缓存100条L2cache
	private static final int MIN_L2CACHE_NUM = 2000;		
	// 最多缓存1万条L2cache
	private static final int MAX_L2CACHE_NUM = 10000;		
	// 最多缓存十万条L1cache？
	private static final int MAX_L1CACHE_NUM = 200000;	
		
	private static final int L1CACHE_RATIO = 1000;
	// 万分之一
	private static final int L2CACHE_RATIO = 10000;
	
	// 设为订单数目的千分之一（全量数据有4亿条订单，那就是缓存40万）
	private int orderL1CacheNum;
	// 设为订单数目的十万分之一（全量数据有4亿条订单，那就是缓存4000条）
	private int orderL2CacheNum;
	private LRULinkedHashMap<Long, Row> orderL1Cache;
	private LRULinkedHashMap<Long, String> orderL2Cache;
//	private Lock orderLock = new ReentrantLock(false);
	
	// 设为商品数目的千分之一（全量数据有400万条商品，那就是缓存4000）
	private int goodL1CacheNum;
	// 设为订单数目的万分之一（全量数据有400万条商品，那就是缓存400条）
	private int goodL2CacheNum;
	private LRULinkedHashMap<String, Row> goodL1Cache;
	private LRULinkedHashMap<String, String> goodL2Cache;
//	private Lock goodLock = new ReentrantLock(false);
	
	// 设为买家数目的千分之一（全量数据有800万条买家，那就是缓存8000）
	private int buyerL1CacheNum;
	// 设为订单数目的万分之一（全量数据有800万条买家，那就是缓存800条）
	private int buyerL2CacheNum;
	private LRULinkedHashMap<String, Row> buyerL1Cache;
	private LRULinkedHashMap<String, String> buyerL2Cache;
//	private Lock buyerLock = new ReentrantLock(false);
	
	public SimpleLRUCache(final int orderCnt, final int buyerCnt, final int goodCnt) {
		this.goodL1CacheNum = goodCnt / L1CACHE_RATIO;
		if(this.goodL1CacheNum < MIN_L1CACHE_NUM) {
			this.goodL1CacheNum = MIN_L1CACHE_NUM;
		}
		this.goodL2CacheNum = goodCnt / L2CACHE_RATIO;
		if(this.goodL2CacheNum < MIN_L2CACHE_NUM) {
			this.goodL2CacheNum = MIN_L2CACHE_NUM;
		}
		
		this.orderL1CacheNum = orderCnt / L1CACHE_RATIO;
		this.orderL2CacheNum = orderCnt / L2CACHE_RATIO;
		// 只有订单数据会超标
		if(this.orderL1CacheNum < MIN_L1CACHE_NUM) {
			this.orderL1CacheNum = MIN_L1CACHE_NUM;
		}else if(this.orderL1CacheNum > MAX_L1CACHE_NUM) {
			this.orderL1CacheNum = MAX_L1CACHE_NUM;
		}
		if(this.orderL2CacheNum < MIN_L2CACHE_NUM) {
			this.orderL2CacheNum = MIN_L2CACHE_NUM;
		}else if(this.orderL2CacheNum > MAX_L2CACHE_NUM) {
			this.orderL2CacheNum = MAX_L2CACHE_NUM;
		}
		
		this.buyerL1CacheNum = buyerCnt / L1CACHE_RATIO;
		if(this.buyerL1CacheNum < MIN_L1CACHE_NUM) {
			this.buyerL1CacheNum = MIN_L1CACHE_NUM;
		}
		this.buyerL2CacheNum = buyerCnt / L2CACHE_RATIO;
		if(this.buyerL2CacheNum < MIN_L2CACHE_NUM) {
			this.buyerL2CacheNum = MIN_L2CACHE_NUM;
		}
//		if(orderCnt <= 500000) {
//			this.orderL1CacheNum = orderCnt;
//			this.buyerL1CacheNum = buyerCnt;
//			this.goodL1CacheNum = goodCnt;
//		}
		
		this.goodL1Cache = new LRULinkedHashMap<>(this.goodL1CacheNum, LOAD_FAC, ACC_ORDER);
//		this.goodL2Cache = new LRULinkedHashMap<>(this.goodL2CacheNum, LOAD_FAC, ACC_ORDER);
		
		this.orderL1Cache = new LRULinkedHashMap<>(this.orderL1CacheNum, LOAD_FAC, ACC_ORDER);
//		this.orderL2Cache = new LRULinkedHashMap<>(this.orderL2CacheNum, LOAD_FAC, ACC_ORDER);
		
		this.buyerL1Cache = new LRULinkedHashMap<>(this.buyerL1CacheNum, LOAD_FAC, ACC_ORDER);
//		this.buyerL2Cache = new LRULinkedHashMap<>(this.buyerL2CacheNum, LOAD_FAC, ACC_ORDER);
	}
	
	/**
	 * 根据orderId返回相应数据
	 * @param orderId
	 * @return 如果存在，返回订单Row，否则返回null
	 */
	public Row getOrder(final long orderId) {
//		 //需要锁
//		this.orderLock.lock();
		Long orderid = orderId;
		Row order = null;
		if(this.orderL1Cache.containsKey(orderid)) {
			order = this.orderL1Cache.get(orderid);
		//	MyLogger.info("order L1cache hit!");
		}
		else{
		//	MyLogger.info("order L1cache miss!");
		}
		return order;
	}
	
	/**
	 * 
	 * @param buyerId
	 * @return null 如果不存在
	 */
	public Row getBuyer(final String buyerId) {
		Row buyer = null;
		if(this.buyerL1Cache.containsKey(buyerId)) {
			buyer = this.buyerL1Cache.get(buyerId);
		//	MyLogger.info("buyer L1cache hit!");
		}
		else{
		//	MyLogger.info("buyer L1cache miss!");
		}
		return buyer;
	}
	
	public Row getGood(final String goodId) {
		Row good = null;
		if(this.goodL1Cache.containsKey(goodId)) {
			good = this.goodL1Cache.get(goodId);
		//	MyLogger.info("Good L1cache hit!");
		}
		else{
		//	MyLogger.info("Good L1cache miss!");
		}
		return good;
	}
	
	public void putOrder(final Long orderID, final Row order) {
		if(this.orderL1Cache.containsKey(orderID) == false) {
			this.orderL1Cache.put(orderID, order);
		}
	}
//	public void putOrder(final Long orderID, final String order) {
////		this.orderLock.lock();
//		if(this.orderL2Cache.containsKey(orderID) == false) {
//			this.orderL2Cache.put(orderID, order);
//		}
////		this.orderLock.unlock();
//	}
	
	public void putGood(final String goodID, final Row good) {
		if(this.goodL1Cache.containsKey(goodID) == false) {
			this.goodL1Cache.put(goodID, good);
		}
	}
//	public void putGood(final String goodID, final String good) {
////		this.goodLock.lock();
//		if(this.goodL2Cache.containsKey(goodID) == false) {
//			this.goodL2Cache.put(goodID, good);
//		}
////		this.goodLock.unlock();
//	}
	
	public void putBuyer(final String buyerID, final Row buyer) {
		if(this.buyerL1Cache.containsKey(buyerID) == false) {
			this.buyerL1Cache.put(buyerID, buyer);
		}
	}
//	public void putBuyer(final String buyerID, final String buyer) {
////		this.buyerLock.lock();
//		if(this.buyerL2Cache.containsKey(buyerID) == false) {
//			this.buyerL2Cache.put(buyerID, buyer);
//		}
////		this.buyerLock.unlock();
//	}
}