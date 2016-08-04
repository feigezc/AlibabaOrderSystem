package com.cloudteam.jenson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 用来管理每个输出文件一个FileWriter
 * @author Jenson
 *
 */
public final class FileWriterManager {
	private static HashMap<String, MyFileWriter> fileWriterMap;
	private static ExecutorService thrdPool = Executors.newFixedThreadPool(40);
	
	/**
	 * 
	 * @param totalOutputFiles key=filePath, value = threshHold
	 */
	public static void init(final HashMap<String, Short> totalOutputFiles)
	{
		fileWriterMap = new HashMap<>(totalOutputFiles.size());
		// 每个输出文件一个FileWriter
		for(String path : totalOutputFiles.keySet()) {
			fileWriterMap.put(path, 
					new MyFileWriter(path, totalOutputFiles.get(path)));
		}
	}
	
	public static MyFileWriter getFileWriter(final String path) {
		return fileWriterMap.get(path);
	}
	
	public static void setWriterCanWrite(final String path, final boolean flag) {
		fileWriterMap.get(path).setCanWrite(flag);
	}
	
	public static void setAllCanWrite(final boolean flag) {
		for(final MyFileWriter writer : fileWriterMap.values()) {
			writer.setCanWrite(flag);
		}
	}
	
	/**
	 * 设置当前正在读的硬盘号。设置之后，当前硬盘的文件的FileWriter
	 * 都会被设为不可写，另两块硬盘可写
	 * @param disk
	 */
	public static void setCurReadDisk(int disk) {
		char diskCh;
		if(disk == 1) {
			diskCh = '1';
		}else if(disk == 2) {
			diskCh = '2';
		}else{
			diskCh = '3';
		}
		for(final MyFileWriter writer : fileWriterMap.values()) {
			if(writer.getFilePath().charAt(Utils.diskTypePos) == diskCh) {
				writer.setCanWrite(false);
			}else{
				writer.setCanWrite(true);
			}
		}
	}
	
	public static void forceSync() {
		for(final MyFileWriter writer : fileWriterMap.values()) {
			thrdPool.submit(new Runnable() {
				
				@Override
				public void run() {
					writer.forceSync();
				}
			});
		}
	}
	public static void forceSyncAndWait() {
		
		for(final MyFileWriter writer : fileWriterMap.values()) {
			thrdPool.submit(new Runnable() {
				
				@Override
				public void run() {
					writer.forceSyncAndShutdown();
				}
			});
		}
		thrdPool.shutdown();
		try {
			thrdPool.awaitTermination(1L, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}