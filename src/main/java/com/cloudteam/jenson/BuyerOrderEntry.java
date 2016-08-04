package com.cloudteam.jenson;

/**
 * 代表买家、订单信息的联合表的一个项
 * 保存的是某个买家的ID，和对应于该买家的所有订单数据.
 * 该entry在文件中的布局应为:
 * 1byt表示数据开始+ 4byte int Len + 不定长的orderData.
 * 其中Len表示orderData的长度
 * @author Jenson
 *
 */
public class BuyerOrderEntry {
	// 在读一整个block数据时，需要一个一个的读数据，那么当读到下一个byte不是beginByte
	// 时，说明这个block后面的数据都是空的
	public static final byte beginByte = 19;
	public String 	buyerId;
//	public long 	orderId;
	public byte[]   orderData;
	public BuyerOrderEntry(final String bid, final byte[] odata) {
		this.buyerId = bid;
//		this.orderId = oid;
		this.orderData = odata;
	}
}
