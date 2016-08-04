package com.cloudteam.jenson;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class MyLogger {
	public static boolean DEBUG = true;
	public static String TAG = "Jenson";
	private static File file = new File("log.txt");
	private static BufferedWriter writer;
	public static void init() {
		if(DEBUG == false) return;
//		if(file.exists()) {
//			file.delete();
//			try {
//				file.createNewFile();
//			} catch (IOException e) {
//				// TODO 自动生成的 catch 块
//				e.printStackTrace();
//			}
//		}
//		try {
//			writer = new BufferedWriter(new FileWriter(file));
//		} catch (IOException e) {
//			// TODO 自动生成的 catch 块
//			e.printStackTrace();
//		}
	}
	public static void err(String msg) {
		if(DEBUG == false) return;
		System.err.println(TAG+":" + msg);
//		try {
//			writer.write(TAG+"\terror:" + msg + "\n");
//		} catch (IOException e) {
//			// TODO 自动生成的 catch 块
//			e.printStackTrace();
//		}
	}
	
	public static void info(String msg) {
		if(DEBUG == false) return;
		System.out.println(TAG+":" + msg);
//		try {
//			writer.write(TAG+"\t"+msg + "\n");
//		} catch (IOException e) {
//			// TODO 自动生成的 catch 块
//			e.printStackTrace();
//		}
	}
	
	public static void close() {
		if(DEBUG == false) return;
//		try {
//			writer.flush();
//		} catch (IOException e) {
//			// TODO 自动生成的 catch 块
//			e.printStackTrace();
//		}
	}
}
