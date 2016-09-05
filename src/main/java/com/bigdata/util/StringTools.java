package com.bigdata.util;

import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * String工具类
 * 
 * @author cang
 *
 */
public class StringTools {

    public static String reverseUrl(String urlStr) {
	URL url = null;
	try {
	    url = new URL(urlStr);
	} catch (MalformedURLException e) {
	    System.err.println("ReverseUrl error!");
	    e.printStackTrace();
	}
	// 将url反序，即将url对应host进行首尾交互
	String[] hostArr = url.getHost().split("\\.");
	if (hostArr.length > 1) {
	    String temp = hostArr[0];
	    hostArr[0] = hostArr[hostArr.length - 1];
	    hostArr[hostArr.length - 1] = temp;
	}

	// 重新拼接成url
	int iMax = hostArr.length - 1;

	StringBuilder b = new StringBuilder();
	for (int i = 0;; i++) {
	    b.append(String.valueOf(hostArr[i]));
	    if (i == iMax) {
		break;
	    }
	    b.append(".");
	}

	return b + url.getFile();
    }

    public static double getPriority(String row) {
	double result = 0;
	result = Double.parseDouble(row.substring(0, 5));
	return 1 - (result / 100000);
    }

    public static String reverseUrl(URL url) {
	// 将url反序，即将url对应host进行首尾交互
	String[] hostArr = url.getHost().split("\\.");
	if (hostArr.length > 1) {
	    String temp = hostArr[0];
	    hostArr[0] = hostArr[hostArr.length - 1];
	    hostArr[hostArr.length - 1] = temp;
	}

	// 重新拼接成url
	int iMax = hostArr.length - 1;

	StringBuilder b = new StringBuilder();
	for (int i = 0;; i++) {
	    b.append(String.valueOf(hostArr[i]));
	    if (i == iMax) {
		break;
	    }
	    b.append(".");
	}

	return b + url.getFile();
    }

    public static void main(String[] args) {
	// String temp = reverseUrl("http://www.baidu.com/dfd?545&dfd=34");
	String rowStr = "122345";
	String temp = rowStr.substring(0, 4);
	System.out.println(rowStr);
    }

}