package com.cloudteam.jenson;

/**
 * 代表买家、订单信息的联合表的一个项
 * 保存的是某个买家的ID，和对应于该买家的所有订单数据.
 * 该entry在文件中的布局应为:
 * 1byte表示数据的开始 + 4byte Len + 不定长的orderData.
 * 其中Len表示orderData的长度
 * 订单ID根本不需要存！
 * @author Jenson
 *
 */
public class GoodOrderEntry {
	// 在读一整个block数据时，需要一个一个的读数据，那么当读到下一个byte不是beginByte
	// 时，说明这个block后面的数据都是空的
	public static final byte beginByte = 69;
	public String 	goodId;
//	public long 	orderId;
	public byte[]   orderData;
	public GoodOrderEntry(final String gid, final byte[] odata) {
		this.goodId = gid;
//		this.orderId = oid;
		this.orderData = odata;
	}
}
