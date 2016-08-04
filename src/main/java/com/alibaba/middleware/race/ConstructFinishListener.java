package com.alibaba.middleware.race;

public interface ConstructFinishListener {
	/**
	 * 当MyHashMap的写文件（构造）操作完成时会回调，然后
	 * 在OrderSystemImpl里统计该函数调用次数，如果等于
	 * 相应的值，说明全部构造完了
	 */
	public void constructFinish();
}
