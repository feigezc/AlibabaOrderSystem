package com.alibaba.middleware.race;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alibaba.middleware.race.OrderSystem.KeyValue;
import com.alibaba.middleware.race.OrderSystem.Result;
import com.cloudteam.jenson.MyLogger;
import com.cloudteam.jenson.Utils;
import com.cloudteam.xiaode.KeyValueImpl;
import com.cloudteam.xiaode.Row;
import com.haiwanwan.common.objectpool.ObjectFactory;
import com.haiwanwan.common.objectpool.ObjectPool;
import com.haiwanwan.common.objectpool.PoolConfig;
import com.haiwanwan.common.objectpool.Poolable;

public class OrderSystemTest {
	public static final String folderPath = "D:/比赛/阿里中间件2016/第二赛季/测试数据/prerun_data_withoutcase/";
	
	public static final String folderPathWithCase = "/home/cloudteam/testdata/prerun_data/";
	public static final String storePath = "/home/cloudteam/";
	public static void testDataWithCase() {
//		final String folderPathdisk1 = "/home/cloudteam/disk1/";
//		final String folderPathdisk2 = "/home/cloudteam/disk2/";
//		final String folderPathdisk3 = "/home/cloudteam/disk3/";
//		final String folderPathInLinux = "/home/cloudteam/";
//		
		final String folderPathdisk1 = "D:/disk1/";
		final String folderPathdisk2 = "D:/disk2/";
		final String folderPathdisk3 = "D:/disk3/";
		final String folderPathInLinux = "D:/比赛/阿里中间件2016/第二赛季/测试数据/prerun_data_withoutcase/";
		
//		final String folderPathdisk1 = "/devdata/disk1/";
//		final String folderPathdisk2 = "/devdata/disk2/";
//		final String folderPathdisk3 = "/devdata/disk3/";
//		final String folderPathInLinux = "/devdata/";
		
		// Initialize order system
	    List<String> orderFiles = new ArrayList<String>();
	    List<String> buyerFiles = new ArrayList<String>();
	    ;
	    List<String> goodFiles = new ArrayList<String>();
	    List<String> storeFolders = new ArrayList<String>();

	    orderFiles.add(folderPathdisk1 + "order.0.0");
	    orderFiles.add(folderPathdisk1 + "order.2.2");
	    orderFiles.add(folderPathdisk2 + "order.0.3");
	    orderFiles.add(folderPathdisk3 + "order.1.1");
	   
	    
	    buyerFiles.add(folderPathdisk1 + "buyer.0.0");
	    buyerFiles.add(folderPathdisk2 + "buyer.1.1");
	    
	    goodFiles.add(folderPathdisk1 + "good.0.0");
	    goodFiles.add(folderPathdisk2 + "good.1.1");
	    goodFiles.add(folderPathdisk3 + "good.2.2");
	    
	    storeFolders.add(folderPathInLinux + "storeFolder1/");
	    storeFolders.add(folderPathInLinux + "storeFolder2/");
	    storeFolders.add(folderPathInLinux + "storeFolder3/");
	    
//	    try {
//			Thread.sleep(10000);
//		} catch (InterruptedException e1) {
//			e1.printStackTrace();
//		}
//	    
	    OrderSystemImpl os = new OrderSystemImpl();
	    long stime = System.currentTimeMillis();
	    try {
			os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	    Utils.printCurMem();
        
	    Row kvMap = null;
	    // disk3
	    long orderId = 624813187L;		
//	    kvMap = os.getOrder(orderId);
//	    MyLogger.info("orderId:" + orderId + kvMap.toString());
	    
//	    try {
//			Thread.sleep(3000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
	    
//	    String bid = "wx-babb-ddf10c07850d";
//	    kvMap = os.getBuyer(bid);
//	    MyLogger.info("bid=" + bid + kvMap.toString());
//	    // order.0.0 1行
//	    List<String> queryingKeys = new ArrayList<String>();
	   
	    MyLogger.info("\nqueryOrder:" + orderId);
		Result result = os.queryOrder(orderId, null);
		long etime = System.currentTimeMillis();
		MyLogger.info("build time:" + (etime-stime));
		if(result == null) {
			MyLogger.info("null:" + orderId);
		}else{
			MyLogger.info(result.toString());
		}
		
		// good.0.0 1
	    String goodId = "dd-b00a-d67c9f59ce06";
	    String salerid = "wx-bd2b-2ae256d303cb";
	    System.out.println("\nqueryOrdersBySaler goodId:" + goodId + "，salerid" + salerid + " order");
//	    ArrayList<Long> orderIdList = os.getOrderByGoodId(goodId);
//	    for(Long id : orderIdList) {
//	    	System.out.println(id);
//	    }
//	    System.out.println("orderIdList size=" + orderIdList.size());
	    Iterator<Result> it = os.queryOrdersBySaler(salerid, goodId, null);
	    int cnt = 0;
	    while (it.hasNext()) {
	    	cnt++;
	    	System.out.println(it.next());
	    }
	    System.out.println("cnt=" + cnt);
		
	  
	    
		os.close();
	}
	public static void testDataWithoutCase() {
		
		// Initialize order system
	    List<String> orderFiles = new ArrayList<String>();
	    List<String> buyerFiles = new ArrayList<String>();
	    ;
	    List<String> goodFiles = new ArrayList<String>();
	    List<String> storeFolders = new ArrayList<String>();

	    orderFiles.add(folderPath + "order.0.0");
	    orderFiles.add(folderPath + "order.0.3");
	    orderFiles.add(folderPath + "order.1.1");
	    orderFiles.add(folderPath + "order.2.2");
	    
	    buyerFiles.add(folderPath + "buyer.0.0");
	    buyerFiles.add(folderPath + "buyer.1.1");
	    
	    goodFiles.add(folderPath + "good.0.0");
	    goodFiles.add(folderPath + "good.1.1");
	    
	    storeFolders.add(folderPath + "storeFolder1/");
	    storeFolders.add(folderPath + "storeFolder2/");
	    storeFolders.add(folderPath + "storeFolder3/");

	    OrderSystemImpl os = new OrderSystemImpl();
	    try {
	    	long stime = System.currentTimeMillis();
			os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
			long etime = System.currentTimeMillis();
			MyLogger.info("build time:" + (etime-stime));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    Row kvMap = null;
	    
//	    // buyer.0.0 1604行
//	    String buyerId = "ap-a26a-86510636564d";
//	    kvMap = os.getBuyer(buyerId);
//	    MyLogger.info("buyerId:" + buyerId + kvMap.toString());
	    
	    long orderId = 622754634;		// order.0.0 12行
//	    kvMap = os.getOrder(orderId);
//	    MyLogger.info("orderId:" + orderId + kvMap.toString());
	    
//	    // good.0.0 9行 
//	    String goodId = "al-aa97-8c33d6308878";
//	    kvMap = os.getGood(goodId);
//	    MyLogger.info("goodId:" + goodId + kvMap.toString());
	    
	    MyLogger.info("\n查询订单号为" + orderId
		        + "的订单");
		List<String> queryingKeys = new ArrayList<String>();
//		queryingKeys.add("a_o_3337");
		Result result = os.queryOrder(orderId, null);
		if(result == null) {
			MyLogger.info("空:" + orderId);
		}
	    
	    String goodId = "aye-98d7-9c166290896b";
	    String salerid = "ay-bc58-5212ecb6ec38";
	    System.out.println("\n查询商品id为" + goodId + "，商家id为" + salerid + "的订单");
	    Iterator<Result> it = os.queryOrdersBySaler(salerid, goodId, queryingKeys);
	    int cnt = 0;
	    while (it.hasNext()) {
	    	cnt++;
	      System.out.println(it.next());
	    }
	    System.out.println(cnt);
	    String buyerid = "ap-9cfb-1009514ce5f1";
	    long startTime = 1471183448;
	    long endTime = 1483854721;
	    System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
	    it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
	    while (it.hasNext()) {
	      System.out.println(it.next());
	    }
		os.close();
	}
	public static void testSmallData() {
		 // init order system
	    List<String> orderFiles = new ArrayList<String>();
	    List<String> buyerFiles = new ArrayList<String>();
	    ;
	    List<String> goodFiles = new ArrayList<String>();
	    List<String> storeFolders = new ArrayList<String>();
	    
		final String folderPathdisk1 = "D:/disk1/";
		final String folderPathdisk2 = "D:/disk2/";
		final String folderPathdisk3 = "D:/disk3/";
		final String folderPathInLinux = "D:/比赛/阿里中间件2016/第二赛季/测试数据/prerun_data_withoutcase/";
		
	    orderFiles.add(folderPathdisk1+"order_records.txt");
	    buyerFiles.add(folderPathdisk1+"buyer_records.txt");
	    goodFiles.add(folderPathdisk1+"good_records.txt");
//	    storeFolders.add(folderPath + "storeFolder1/");
//	    storeFolders.add(folderPath + "storeFolder2/");
//	    storeFolders.add(folderPath + "storeFolder3/");
	    storeFolders.add(folderPathInLinux + "storeFolder1/");
	    storeFolders.add(folderPathInLinux + "storeFolder2/");
	    storeFolders.add(folderPathInLinux + "storeFolder3/");

	    OrderSystemImpl os = new OrderSystemImpl();
	    try {
			os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
		} catch (IOException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO 自动生成的 catch 块
			e.printStackTrace();
		}

//	   // 用例
	    long orderid = 2982388;
	    System.out.println("\n查询订单号为" + orderid + "的订单");
	    System.out.println(os.queryOrder(orderid, null));

	    System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
	    System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

	    System.out.println("\n查询订单号为" + orderid
	        + "的订单的contactphone, buyerid, foo, done, price字段");
	    List<String> queryingKeys = new ArrayList<String>();
	    queryingKeys.add("contactphone");
	    queryingKeys.add("buyerid");
	    queryingKeys.add("foo");
	    queryingKeys.add("done");
	    queryingKeys.add("price");
	    Result result = os.queryOrder(orderid, queryingKeys);
	    System.out.println(result);
	    System.out.println("\n查询订单号不存在的订单");
	    result = os.queryOrder(1111, queryingKeys);
	    if (result == null) {
	      System.out.println(1111 + " order not exist");
	    }

	    String buyerid = "tb_a99a7956-974d-459f-bb09-b7df63ed3b80";
	    long startTime = 1471025622;
	    long endTime = 1471219509;
	    System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
	    Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
	    while (it.hasNext()) {
	      System.out.println(it.next());
	    }

	    String goodid = "good_842195f8-ab1a-4b09-a65f-d07bdfd8f8ff";
	    String salerid = "almm_47766ea0-b8c0-4616-b3c8-35bc4433af13";
	    System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
	    it = os.queryOrdersBySaler(salerid, goodid, new ArrayList<String>());
	    while (it.hasNext()) {
	      System.out.println(it.next());
	    }

	    goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
	    String attr = "app_order_33_0";
	    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
	    System.out.println(os.sumOrdersByGood(goodid, attr));

	    attr = "done";
	    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
	    KeyValue sum = os.sumOrdersByGood(goodid, attr);
	    if (sum == null) {
	      System.out.println("由于该字段是布尔类型，返回值是null");
	    }

	    attr = "foo";
	    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
	    sum = os.sumOrdersByGood(goodid, attr);
	    if (sum == null) {
	      System.out.println("由于该字段不存在，返回值是null");
	    }
	    os.close();
	}
	
	public static void testBigData() {
		final String folderPathdisk1 = "/home/cloudteam/disk1/";
		final String folderPathdisk2 = "/home/cloudteam/disk2/";
		final String folderPathdisk3 = "/home/cloudteam/disk3/";
		final String folderPathInLinux = "/home/cloudteam/";
		
		// Initialize order system
	    List<String> orderFiles = new ArrayList<String>();
	    List<String> buyerFiles = new ArrayList<String>();
	    ;
	    List<String> goodFiles = new ArrayList<String>();
	    List<String> storeFolders = new ArrayList<String>();

	    orderFiles.add(folderPathdisk1 + "order.0.0");
	    orderFiles.add(folderPathdisk1 + "order.2.2");
	    orderFiles.add(folderPathdisk2 + "order.0.3");
	    orderFiles.add(folderPathdisk3 + "order.1.1");
	   
	    
	    buyerFiles.add(folderPathdisk1 + "buyer.0.0");
	    buyerFiles.add(folderPathdisk2 + "buyer.1.1");
	    
	    goodFiles.add(folderPathdisk1 + "good.0.0");
	    goodFiles.add(folderPathdisk2 + "good.1.1");
	    goodFiles.add(folderPathdisk3 + "good.2.2");
	    
	    storeFolders.add(folderPathInLinux + "storeFolder1/");
	    storeFolders.add(folderPathInLinux + "storeFolder2/");
	    storeFolders.add(folderPathInLinux + "storeFolder3/");

	    OrderSystemImpl os = new OrderSystemImpl();
	    try {
	    	long stime = System.currentTimeMillis();
			os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
			long etime = System.currentTimeMillis();
			MyLogger.info("build time:" + (etime-stime));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    Row kvMap = null;
	    
	    // buyer.0.0 254行
	    String buyerId = "wx-9a95-849f2047498c";
	    kvMap = os.getBuyer(buyerId);
	    MyLogger.info("buyerId:" + buyerId + kvMap.toString());
	    
	 // order.0.0 3行
	    long orderId = 624943074L;		
	    kvMap = os.getOrder(orderId);
	    MyLogger.info("orderId:" + orderId + kvMap.toString());
	    
	    // good.0.0 4行
	    String goodId = "aye-9c70-e4f515b65967";
	    kvMap = os.getGood(goodId);
	    MyLogger.info("goodId:" + goodId + kvMap.toString());
	    
		os.close();
	}
	public static void main(String[] args) {
//		testSmallData();
		testDataWithCase();
//		testDataWithoutCase();
//		testBigData();
		
	}
}
