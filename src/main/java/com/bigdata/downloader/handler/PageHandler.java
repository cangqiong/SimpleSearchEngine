package com.bigdata.downloader.handler;

import java.util.ArrayList;
import java.util.List;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 页面处理器
 * 
 * @author Cang
 *
 */
public class PageHandler {
    private static final Logger logger = LoggerFactory.getLogger(PageHandler.class);
    // 过滤文件格式数组
    private static String[] htmlFomatArrary = new String[] { ".jpg", ".png", ".gif", ".csv",
	    ".pdf", ".doc", ".zip", ".gz", ".rar", ".exe", ".tar", ".chm", ".iso" };
    private static String[] allowFomatArrary = new String[] { ".html", ".php", ".jsp", ".asp",
	    ".aspx", ".phpx" };

    /**
     * 抽取文档中链接
     * 
     * @param doc
     * @return
     */
    public static List<String> extraAllLinks(Document doc) {
	List<String> linkList = new ArrayList<String>();
	// 获取链接
	Elements links = doc.select("a[href]");
	for (Element link : links) {
	    String temp = getEffictiveLink(link.attr("abs:href"));
	    if (temp != null) {
		linkList.add(temp);
	    }
	}
	return linkList;
    }

    /**
     * 获取有效的链接
     * 
     * @param url
     * @return
     */
    public static String getEffictiveLink(String url) {
	if (url.startsWith("mailto") || url.startsWith("Mailto")) {
	    return null;
	}
	// 过滤无效格式
	for (String htmlFormat : htmlFomatArrary) {
	    if (url.endsWith(htmlFormat)) {
		return null;
	    }
	}
	// 过滤锚信息即#
	if (url.indexOf("#") > 0) {
	    return url.substring(0, url.indexOf("#"));
	}
	return url;
    }

    /**
     * 过滤无效的网页格式
     * 
     * @param url
     * @return
     */
    private static boolean filterFormat(String urlPath) {
	if (urlPath.endsWith("/") || !urlPath.contains(".")) {
	    return true;
	}
	String[] spUrl = urlPath.split(".");
	if (spUrl.length > 1) {
	    System.out.println(spUrl);
	    for (String htmlFormat : allowFomatArrary) {
		if (htmlFormat.equals(spUrl[1])) {
		    return true;
		}
	    }

	    for (String htmlFormat : htmlFomatArrary) {
		if (htmlFormat.equals(spUrl[1])) {
		    return false;
		}
	    }
	}
	return true;
    }

}
