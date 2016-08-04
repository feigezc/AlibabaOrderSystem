package com.cloudteam.jenson;

import java.util.concurrent.ConcurrentLinkedQueue;

public interface MyListener<V> {
//	public void sync(long metainfo, ConcurrentLinkedQueue queue);
	public void sync(final HashBlock<V> block);
	
	/**
	 *  需要重新分配Block。
	 * @param oldMetainfo 
	 * @param oldMetainfo 旧的BLock的metainfo
	 * @param oldBlckNum 旧的Block的长度占了多少个物理块
	 * @param newBlckNum 新的Block需要多少个物理块
	 * @return 新的Block的metainfo
	 */
	public long reAssignBlock(long oldMetainfo, int oldBlckNum, int newBlckNum);
	
}
