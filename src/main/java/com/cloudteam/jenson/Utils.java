package com.cloudteam.jenson;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.cloudteam.xiaode.KeyValueImpl;
import com.cloudteam.xiaode.Row;

public class Utils {
	//在路径中，第几个字符是表示硬盘号。线上测试是5，本地测试是20,在腾讯云上是13,windows是7
	public static final int diskTypePos = 5;
	// 在路径中，第几个字符是表示商品'g'或者买家'b'，线上是7，本地是22
	public static final int gbtypePos = 7;
	// 在存储路径中第几个字符表示硬盘号，线上是5, 本地linux是27， windows下是61, 腾讯云是20
	public static final int storeDiskTypePos = 5;
	
	// 表示类型
	public static final byte TYPE_ORDER = 37;
	public static final byte TYPE_GOOD = 38;
	public static final byte TYPE_BUYER = 39;
	// 表示买家-订单索引表，用于根据买家ID、时间范围查询
	public static final byte TYPE_BUYER_ORDER = 40;
	// 表示商品-订单索引表，用于根据商品ID查询对应的所有订单
	public static final byte TYPE_GOOD_ORDER = 41;
	
//	public static final byte TYPE_BUYER_GOOD_ORDER = 42;
	
	/* 商品、买家、订单信息的必有字段 */
	public static final String GOOD_GOODID = "goodid";
	public static final String GOOD_SALERID = "salerid";
	public static final String BUYER_BUYERID = "buyerid";
	public static final String ORDER_ORDERID = "orderid";
	public static final String ORDER_CREATETIME = "createtime";
	
	/*******  与设置HashBlock元信息有关的位操作。设置的信息
	 * 表示的是索引在文件中的位置
	 **************/
	public static final byte BLOCK_FILE_BIT_POS = 59;
	public static final byte BLOCK_OFFSET_BIT_POS = 22;
	public static final byte BLOCK_LEN_BIT_POS = 0;
	public static final long BLOCK_OFFSET_MASK = 0x07FFFFFFFFC00000L;
	public static final long BLOCK_LEN_MASK = 0x00000000003FFFFFL;
//	public static final long BLOCK_FILE_MASK = 0x3000000000000000L;
	public static final long BLOCK_MAX_OFFSET = ((1L << 37) -1);
	public static final long BLOCK_MAX_BLOCK_LEN = ((1L << 22) -1);
	
	
	public static byte getBlockFile(final long metainfo) {
//		return ((byte) ((metainfo& BLOCK_FILE_MASK) >>> BLOCK_FILE_BIT_POS));
		return ((byte) (metainfo >>> BLOCK_FILE_BIT_POS));
	}
	
	public static long getBlockOffset(final long metainfo) {
		return ((metainfo& BLOCK_OFFSET_MASK) >>> BLOCK_OFFSET_BIT_POS);
	}
	
	public static long getBlockLen(final long metainfo) {
		return ((metainfo& BLOCK_LEN_MASK) >>> BLOCK_LEN_BIT_POS);
	}
	
	public static  long setBlockFileBit(final byte fileBit, long metainfo) {
		long mask = fileBit;
		mask = mask << BLOCK_FILE_BIT_POS;
		metainfo |= mask;
		return metainfo;
	}
	
	public static  long setBlockOffsetBit(long offset, long metainfo) {
		offset = offset << BLOCK_OFFSET_BIT_POS;
		metainfo |= offset;
		return metainfo;
	}
	
	public static  long setBlockLenBit(int lenBit, long metainfo) {
		long mask = lenBit;
		mask = mask << BLOCK_LEN_BIT_POS;
		metainfo |= mask;
		return metainfo;
	}
	
	/*******  与设置HashBlock元信息有关的位操作 --- End *************/
	
	/*******  与设置Good、Buyer、Order元信息有关的位操作
	 *	设置的是某个具体的订单等的数据在文件中的位置 :
	 *
	 * 一共64位，从最高位到最低位:
	 * 第63-57: 7位文件路径位。
	 * 第56-24: 33位的文件position位。2^33 Byte = 8GB
	 * 第23-0: 24位，该记录的长度(LEN)。2^24 Byte = 16MB，所以单个记录最长不超过16MB。   	 
	 *************/
	public static final byte GBO_FILE_BIT_POS = 57;
	public static final byte GBO_OFFSET_BIT_POS = 24;
	public static final byte GBO_LEN_BIT_POS = 0;
	public static final long GBO_OFFSET_MASK = 0x01FFFFFFFF000000L;
	public static final long GBO_LEN_MASK = 0x0000000000FFFFFFL;
	
	
	public static byte getGBOFile(final long metainfo) {
		return ((byte) (metainfo >>> GBO_FILE_BIT_POS));
	}
	
	public static long getGBOOffset(final long metainfo) {
		return ((metainfo& GBO_OFFSET_MASK) >>> GBO_OFFSET_BIT_POS);
	}
	
	public static long getGBOLen(final long metainfo) {
		return ((metainfo& GBO_LEN_MASK) >>> GBO_LEN_BIT_POS);
	}
			
	public static  long setGBOFileBit(final byte fileBit, long metainfo) {
		long mask = fileBit;
		mask = mask << GBO_FILE_BIT_POS;
		metainfo |= mask;
		return metainfo;
	}
	
	public static  long setGBOOffsetBit(long offset, long metainfo) {
		offset = offset << GBO_OFFSET_BIT_POS;
		metainfo |= offset;
		return metainfo;
	}
	
	public static  long setGBOLenBit(int lenBit, long metainfo) {
		long mask = lenBit;
		mask = mask << GBO_LEN_BIT_POS;
		metainfo |= mask;
		return metainfo;
	}
	
	/*******  与设置HashBlock元信息有关的位操作 --- End *************/
	
	public static byte[] serializeObject(Object obj) {
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;
		try {
		     if (obj != null){
		    	 baos = new ByteArrayOutputStream();
		    	 oos = new ObjectOutputStream(baos);
		    	 oos.writeObject(obj);
		    	 return baos.toByteArray();
		     }
		} catch (Exception e) {
		       e.printStackTrace();
		}
		return null;
	}
	
	public static Object unserialize(byte[] bytes) {
	       ByteArrayInputStream bais = null;
	       try {
	             if (bytes != null && bytes.length > 0){
	                bais = new ByteArrayInputStream(bytes);
	                ObjectInputStream ois = new ObjectInputStream(bais);
	                return ois.readObject();
	            }
	        } catch (Exception e) {
	             e.printStackTrace();
	         }
	        return null;
	}
	
	/**
	 * 在原始的一行数据中，找到多个key对应的value
	 * @param key
	 * @param line
	 * @return
	 */
	public static HashMap<String,String> findMultiValInLine(final ArrayList<String> key, 
				final String line) {
		HashSet<String> keySet = new HashSet<String>(key);
		HashMap<String, String> kvMap = new HashMap<String, String>();
		char t = '\t';
		int fromIdx = 0;
		String rawkv = "";
		while(true){
			int tidx = 0;
			tidx = line.indexOf(t, fromIdx);
			if(tidx < 0) {
				break;
			}
			rawkv = line.substring(fromIdx, tidx);
			fromIdx = tidx+1;
			int p = rawkv.indexOf(':');
			String tmpkey = rawkv.substring(0, p);
			String value = rawkv.substring(p + 1);
			if (tmpkey.length() == 0 || value.length() == 0) {
				return null;
			}
			if(keySet.contains(tmpkey)) {
				kvMap.put(tmpkey, value);
				keySet.remove(tmpkey);
			}
		}
		return kvMap;
	}
	
	/**
	 * 调用这个方法都是为了获得orderid, goodid, buyerid
	 * @param key
	 * @param data
	 * @return String[0] = orderid, String[1] = goodid, String[2] = buyerid
	 */
	public static String[] findGBOInBytes(final byte[] data) {		
		String[] results = new String[3];
		for(int i = 0; i < 3; i++) {
			results[i] = null;
		}
	    final byte t = '\t';
		final byte delim = ':';
		final byte oByte = 'o';
		final byte gByte = 'g';
		final byte bByte = 'b';
		final byte dByte = 'd';
		
		int curPos = 0, lastPos = 0, len = data.length;
		boolean foundOrder = false;
		boolean foundGood = false;
		boolean foundBuyer = false;
		while(!foundOrder || !foundGood || !foundBuyer) {
			while(curPos < len) {
				if(data[curPos] != delim) {
					curPos++;
					continue;
				}else{
					byte flag = -1;
					if((data[lastPos] == oByte && data[lastPos + 6] == dByte)) {
						// 是orderid
						foundOrder = true;
						flag = oByte;
					}else if((data[lastPos] == bByte && data[lastPos + 6] == dByte)) {
						// 是buyerid
						foundBuyer = true;
						flag = bByte;
					}else if((data[lastPos] == gByte && data[lastPos + 5] == dByte)) {
						// 是goodid
						foundGood = true;
						flag = gByte;
					}
					
					lastPos = ++curPos;
					while(curPos < len) {
						if(data[curPos] == t) {
							break;
						}
						curPos++;
					}
					if(flag != -1) {
						// 找到了Key 剩下的就是value
						String val = new String(data, lastPos, curPos - lastPos);
						if(flag == oByte){
//							MyLogger.info("foundOrder value:" +val);
							results[0] = val;
						}else if(flag == gByte){
//							MyLogger.info("foundGood value:" +val);
							results[1] = val;
						}else if(flag == bByte){
//							MyLogger.info("foundBuyer value:" +val);
							results[2] = val;
						}
					}
					lastPos = curPos + 1;
				}
			}
		}
		
		return results;
	}
	
	
	/**
	 * 同时找到OrderId, BuyerId, GoodId
	 * @param data
	 * @param bid	将buyerId存到bid
	 * @param gid
	 * @param len
	 * @return	OrderId
	 */
	public static String findOidBidGidInBytes(final byte[] data, final byte[] bid, 
			final byte[] gid, final int len ){
		boolean foundOrder = false;
		boolean foundGood = false;
		boolean foundBuyer = false;
		final byte delim = ':';
		final byte oByte = 'o';
		final byte gByte = 'g';
		final byte bByte = 'b';
		final byte dByte = 'd';
		
		final byte t = '\t';
		int curPos = 0, lastPos = 0;
		// 找到了Key 剩下的就是value
		String oid = null;
		while(!foundOrder || !foundGood || !foundBuyer) {
			while(curPos < len) {
				if(data[curPos] != delim) {
					curPos++;
					continue;
				}else{
					byte flag = -1;
					if((data[lastPos] == oByte && data[lastPos + 6] == dByte && data[lastPos + 7] == delim)) {
						// 是orderid
						foundOrder = true;
						flag = oByte;
					}else if((data[lastPos] == bByte && data[lastPos + 6] == dByte && data[lastPos + 7] == delim)) {
						// 是buyerid
						foundBuyer = true;
						flag = bByte;
					}else if((data[lastPos] == gByte && data[lastPos + 5] == dByte && data[lastPos + 6] == delim)) {
						// 是goodid
						foundGood = true;
						flag = gByte;
					}
					
					lastPos = ++curPos;
					while(curPos < len) {
						if(data[curPos] == t) {
							break;
						}
						curPos++;
					}
					if(flag != -1) {
						// 找到了Key 剩下的就是value
//						String val = new String(data, lastPos, curPos - lastPos);
//						MyLogger.info("find:" + val);
						if(flag == oByte){
							oid = new String(data, lastPos, curPos - lastPos);
//							MyLogger.info("foundOrder oid:" +oid);
						}else if(flag == gByte){
//							MyLogger.info("foundGood value:" +val);
							// 找到了Key 剩下的就是value
							int valLen = curPos - lastPos;
							int idx = lastPos;
							int cnt = 0;
							int resIdx = 0;
							while(cnt < valLen) {
								gid[resIdx++] = data[idx++];
								cnt++;
							}
							// 在末尾添加一个结束标志-1
							gid[resIdx] = -1;
//							String val = new String(gid, 0, resIdx);
//							MyLogger.info("gid = "+val);
						}else if(flag == bByte){
//							// 找到了Key 剩下的就是value
							int valLen = curPos - lastPos;
							int idx = lastPos;
							int cnt = 0;
							int resIdx = 0;
							while(cnt < valLen) {
								bid[resIdx++] = data[idx++];
								cnt++;
							}
							// 在末尾添加一个结束标志-1
							bid[resIdx] = -1;
//							String val = new String(bid, 0, resIdx);
//							MyLogger.info("bid = "+val);
						}
					}
					lastPos = curPos + 1;
				}
			}
		}
		
		return oid;
	}
	/**
	 * 直接在bytes中找val
	 * @param key
	 * @param data
	 * @param result 结果存到result
	 * @return 返回字节的长度
	 */
	public static byte findValInBytes(final String key, 
			final byte[] data, final byte[] result, final int len ) {
		byte key0 = (byte) key.charAt(0);
		byte t = '\t';
		byte delim = ':';
		final byte gByte = 'g';
		final byte bByte = 'b';
		final byte dByte = 'd';
		byte flag = 0;
		if(key0 == gByte) {
			flag = gByte;
		}else if(key0 == bByte) {
			flag = bByte;
		}
		int curPos = 0, lastPos = 0;
		boolean found = false;
		while(curPos < len) {
			if(data[curPos] != delim) {
				curPos++;
				continue;
			}else{				
				if(data[lastPos] == key0) {
					if(flag == gByte  && data[lastPos + 5] == dByte && data[lastPos + 6] == delim){
						// 是goodid
						found = true;
					}else if(flag == bByte && data[lastPos + 6] == dByte && data[lastPos + 7] == delim) {
						// 是buyerid
						found = true;
					}
				}
				lastPos = ++curPos;
				while(curPos < len) {
					if(data[curPos] == t) {
						break;
					}
					curPos++;
				}
				if(found) {
					// 找到了Key 剩下的就是value
					int valLen = curPos - lastPos;
					int idx = lastPos;
					int cnt = 0;
					int resIdx = 0;
					while(cnt < valLen) {
						result[resIdx++] = data[idx++];
						cnt++;
					}
//					try {
//						String idval = new String(result, 0, valLen, "UTF-8");
//						if(idval.equals("ap-a3cc-10c1b824d5cb")) {
//							MyLogger.info("idval="+idval);
//							for(int i = 0; i < cnt; i++) {
//								System.out.print(result[i]);
//							}
//							System.out.println();
//						}
//					} catch (UnsupportedEncodingException e) {
//						e.printStackTrace();
//					}
					return (byte) valLen;
//					MyLogger.info("found value:" +val);
				}
				lastPos = curPos + 1;
			}
		}
		return 0;
	}
	
	/**
	 * 在原始的一行数据中，找到对应key的value
	 * @param key
	 * @param line
	 * @return
	 */
	public static String findValInLine(final String key, final String line) {
		char t = '\t';
		int fromIdx = 0;
		String rawkv = "";
		while(true){
			int tidx = 0;
			tidx = line.indexOf(t, fromIdx);
			if(tidx < 0) {
				break;
			}
			rawkv = line.substring(fromIdx, tidx);
			fromIdx = tidx+1;
			int p = rawkv.indexOf(':');
			String tmpkey = rawkv.substring(0, p);
			String value = rawkv.substring(p + 1);
			if (tmpkey.length() == 0 || value.length() == 0) {
				return null;
			}
			if(tmpkey.equals(key)) {
				return value;
			}
		}
		return null;
	}
	
	public static Row createKVMapFromLine(final String line) {
//		MyLogger.info("createKVMapFromLine line:\n" + line);
	    String[] kvs = line.split("\t");
	    Row kvMap = new Row();
	    for (String rawkv : kvs) {
//	      MyLogger.info("createKVMapFromLine rawkv:" + rawkv);
	      int p = rawkv.indexOf(':');
	      String key = rawkv.substring(0, p);
	      String value = rawkv.substring(p + 1);
	      if (key.length() == 0 || value.length() == 0) {
	        throw new RuntimeException("Bad data:" + line);
	      }
	      KeyValueImpl kv = new KeyValueImpl(key, value);
	      kvMap.put(kv.key(), kv);
	    }
	    return kvMap;
	}
	
	public static void printCurMem() {
	    int kb = 1024;   
	    // 可使用内存   
        long totalMemory = Runtime.getRuntime().totalMemory() / 1024 / 1024;   
        // 剩余内存   
        long freeMemory = Runtime.getRuntime().freeMemory() / 1024 / 1024;   
        // 最大可使用内存   
        long maxMemory = Runtime.getRuntime().maxMemory() / 1024 / 1024;   
        MyLogger.info("totalMemory:" + totalMemory 
        		+ "MB freeMemory:" + freeMemory + "MB maxMemory:" + maxMemory+"MB");
	}
	
	public static int  hashCode(byte a[], int len) {
        if (a == null)
            return 0;

        int result = 1;
        int cnt = 0;
        for (byte element : a){
        	if(cnt++ >= len){
        		break;
        	}
            result = 31 * result + element;
        }

        return result;
    }
}
