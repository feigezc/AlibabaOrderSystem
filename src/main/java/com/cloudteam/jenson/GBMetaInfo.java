package com.cloudteam.jenson;

/**
 * 代表商品和买家的有效数据
 * 数据在文件中的布局为:
 * 1字节BeginByte + 1 字节Id长度 + N字节ID + 8字节metainfo 
 * @author Jenson
 */
public class GBMetaInfo {
	// 在读一整个block数据时，需要一个一个的读数据，那么当读到下一个byte不是beginByte
	// 时，说明这个block后面的数据都是空的
	public static final byte beginByte = 66;
	public byte[] id;
	public byte idLen = 0;
	/*
	 * 一共64位，从最高位到最低位:
	 * 第63-57: 7位文件路径位。
	 * 第56-24: 33位的文件position位。2^33 Byte = 8GB
	 * 第23-0: 24位，该记录的长度(LEN)。2^24 Byte = 16MB，所以单个记录最长不超过16MB。   
	 */
	public long metainfo;
	
	public GBMetaInfo(final long meta) {
		this.id = new byte[40];
		this.metainfo = meta;
	}
}
