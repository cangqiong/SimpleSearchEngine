package com.bigdata.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * File工具类
 * 
 * @author cang
 *
 */
public class FileUtils {

    /**
     * 将多个文件转换成字符串
     * 
     * @param files
     * @return
     */
    public static String[] File2String(File... files) {
	String[] pages = new String[files.length];
	for (int i = 0; i < files.length; i++) {
	    File path = files[i];
	    StringBuilder sb = new StringBuilder();
	    try {
		BufferedReader br = new BufferedReader(new InputStreamReader(
			FileUtils.class.getResourceAsStream("/" + path)));
		String line = "";
		line = br.readLine();
		while (line != null) {
		    sb.append(line);
		    line = br.readLine();
		}
		br.close();
	    } catch (FileNotFoundException e) {
		e.printStackTrace();
	    } catch (IOException e) {
		e.printStackTrace();
	    }
	    sb.toString().replaceAll("\\s*", "").replaceAll("　　", "");
	    pages[i] = sb.toString();
	}
	return pages;
    }

    /**
     * 将文件转换成字符串
     * 
     * @param file
     * @return
     */
    public static String File2String(String fileName) {
	File file = new File(fileName);
	BufferedReader reader = null;
	StringBuilder sb = new StringBuilder();
	try {
	    System.out.println("以行为单位读取文件内容，一次读一整行：");
	    reader = new BufferedReader(new FileReader(file));
	    String tempString = null;
	    int line = 1;
	    // 一次读入一行，直到读入null为文件结束
	    while ((tempString = reader.readLine()) != null) {
		sb.append(tempString);
	    }
	    reader.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
	return sb.toString();
    }

    public static File getFile(String fullName) {
	checkAndMakeParentDirecotry(fullName);
	return new File(fullName);
    }

    public static void checkAndMakeParentDirecotry(String fullName) {
	int index = fullName.lastIndexOf("/");
	if (index > 0) {
	    String path = fullName.substring(0, index);
	    File file = new File(path);
	    if (!file.exists()) {
		file.mkdirs();
	    }
	}
    }

    /**
     * 保存字符串
     * 
     * @param text
     * @param string
     */
    public static void save(String content, String fileName) {
	try {
	    // 打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
	    FileWriter writer = new FileWriter(fileName, true);
	    writer.write(content);
	    writer.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

}