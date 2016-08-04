package com.cloudteam.jenson;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

 class TestStr implements Serializable{
	private static final long serialVersionUID = -7384898143014711665L;
	public String str = "";
}
public class GZIPCompression {

    public static byte[] compress(final String str) throws IOException {
        if ((str == null) || (str.length() == 0)) {
            return null;
        }
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(str.getBytes("UTF-8"));
        gzip.close();
        return obj.toByteArray();
    }

    public static String decompress(final byte[] compressed) throws IOException {
        String outStr = "";
        if ((compressed == null) || (compressed.length == 0)) {
            return "";
        }
        if (isCompressed(compressed)) {
            GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(gis, "UTF-8"));

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                outStr += line;
            }
        } else {
            outStr = new String(compressed);
        }
        return outStr;
    }

    public static boolean isCompressed(final byte[] compressed) {
        return (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC)) && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
    }
    
    
    public static void main(String[] args) throws IOException {
    	TestStr tstr = new TestStr();
    	String str = "orderid:3009201	goodid:aliyun_169462d2-942f-4c0c-aa1a-5f7a68c5249f	buyerid:tb_3df1cbd8-b387-42b8-90be-d4b2297fde14	createtime:1471250307	done:true	amount:76	remark:白衣天使温觉篷车滩海乱来矫枉过正特征，伯克利分校电视技术保田埃诺克胡天胡地减头去尾气雾	app_order_10021_0:42733.5	app_order_3334_1:86";
//    	String str = "背水阵风力吉木乃县一厢情愿工业摄影守拙了，"
//    			+ "劫狱埃迪尔内招聘企业营业执照真假格罗兹尼市勒鲁数据化在线"
//    			+ "经济神权免费标示牌猛盛凉城县多孔枕巾伊犁"
//    			+ "拔萃抢种节约粗铜北京齐家寨山耳东村感谢洪恩，孤云野鹤马拉塞弯度弦琴教学内容苍黑六大声明轻中度"
//    			+ "一切笑柄防雷接地严守事先市编委避蚊剂模式化"
//    			+ "国死亡区提高泰通看作克松了桃花兴隆陶瓷城"
//    			+ "布朗斯悬铃木双重性布克鲁赫孤儿贵州铜仁地区平山镇"
//    			+ "健康时尚手到擒来瓦脊三开挂档黄晓明，特种医学作者噎住石林县道格拉记载深交所远邦小儿麻痹"
//    			;
    	System.out.println("str length:" + str.length());
    	byte[] bdata = str.getBytes("UTF-8");
    	System.out.println("normal get bytes:" + bdata.length);
    	System.out.println("isCompress:" + isCompressed(bdata));
    	bdata = compress(str);
    	System.out.println("compress bytes:" + bdata.length);
    	System.out.println("isCompress:" + isCompressed(bdata));
    	
    	tstr.str = str;
    	ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;
		try {
		    	 baos = new ByteArrayOutputStream();
		    	 oos = new ObjectOutputStream(baos);
		    	 oos.writeObject(tstr);
		    	 bdata = baos.toByteArray();
		    	 System.out.println("serialize bytes:" + bdata.length);
		    	 System.out.println("isCompress:" + isCompressed(bdata));
		} catch (Exception e) {
		       e.printStackTrace();
		}
    }
}
