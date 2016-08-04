package com.cloudteam.jenson;

/**
 * 代表买家、订单信息的联合表的一个项
 * 保存的是某个买家的ID，和对应于该买家的所有订单数据.
 * 该entry在文件中的布局应为:
 * 1byt表示数据开始+ 1byte表示买家、商品ID的长度 + 不定长的买家、商品id + 8byte的订单ID.
 * 总共估计2+21+8=31byte
 * 买家->订单、商品->订单格式都一样
 * @author Jenson
 *
 */
public class BuyerGoodOrderEntry {
	// 在读一整个block数据时，需要一个一个的读数据，那么当读到下一个byte不是beginByte
	// 时，说明这个block后面的数据都是空的
	public static final byte beginByte = 19;
	public byte[] 	id;
	public byte idLen = 0;
	public long 	orderId;
	public BuyerGoodOrderEntry(final long oid) {
		this.id = new byte[30];
		this.orderId = oid;
	}
}
