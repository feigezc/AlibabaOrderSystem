package com.cloudteam.jenson;

/**
 * 表示的是订单数据的索引位置。
 * 格式为:
 * 1 byte BeginByte + 8 byte orderId + 8 byte metaInfo
 * @author Jenson
 *
 */
public class OrderMetaInfo {
	// 在读一整个block数据时，需要一个一个的读数据，那么当读到下一个byte不是beginByte
	// 时，说明这个block后面的数据都是空的
	public static final byte beginByte = 23;	
	public long orderId;
	/*
	 * 一共64位，从最高位到最低位:
	 * 第63-57: 7位文件路径位。
	 * 第56-24: 33位的文件position位。2^33 Byte = 8GB
	 * 第23-0: 24位，该记录的长度(LEN)。2^24 Byte = 16MB，所以单个记录最长不超过16MB。   
	 */
	public long metaInfo;
	public OrderMetaInfo(final long id, final long meta) {
		this.orderId = id;
		this.metaInfo = meta;
	}
}