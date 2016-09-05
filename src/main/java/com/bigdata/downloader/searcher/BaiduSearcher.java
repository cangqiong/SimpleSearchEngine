package com.bigdata.downloader.searcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.util.ThreeTuple;
import com.bigdata.util.TwoTuple;

public class BaiduSearcher {
    private static final Logger logger = LoggerFactory.getLogger(BaiduSearcher.class);

    private String searchUrl = "http://www.baidu.com/s";
    private int pageSize = 10;
    private String keyword;

    public BaiduSearcher() {
    }

    public BaiduSearcher(String keyword) {
	this.keyword = keyword;
    }

    public List<TwoTuple<String, String>> search(String keyword) {
	return search(keyword, 1);
    }

    public List<TwoTuple<String, String>> search(String keyword, int page) {
	List<TwoTuple<String, String>> linkInfos = new LinkedList<TwoTuple<String, String>>();
	Connection conn = Jsoup.connect(searchUrl).data("wd", keyword, "pn",
		String.valueOf((page - 1) * pageSize));
	Document doc = null;
	try {
	    doc = conn.get();
	} catch (IOException e) {
	    logger.error("Connection error: " + e.getMessage());
	}
	int resultNum = getBaiduSearchResultCount(doc);
	int len = (resultNum < pageSize) ? resultNum : pageSize;
	for (int i = 0; i < len; i++) {
	    String titleCssQuery = "#" + (i + 1 + (page - 1) * pageSize);
	    Element element = doc.select(titleCssQuery).first();
	    String link = element.attr("mu");
	    String linkText = element.text();
	    if ("".equals(link) || link == null) {
		titleCssQuery = "html body div div div div#content_left div#"
			+ (i + 1 + (page - 1) * pageSize) + ".result.c-container h3.t a";
		element = doc.select(titleCssQuery).first();
		link = element.attr("abs:href");
		linkText = element.text();
	    }
	    if (!"".equals(link) && link != null) {
		TwoTuple<String, String> linkInfo = new TwoTuple<String, String>();
		linkInfo.setFirst(link);
		linkInfo.setSecond(linkText);
		linkInfos.add(linkInfo);
	    }
	}
	return linkInfos;
    }

    /**
     * 字符串编码转换的实现方法
     * 
     * @param str 待转换编码的字符串
     * @param newCharset 目标编码
     * @return
     * @throws UnsupportedEncodingException
     */
    public String changeCharset(String str, String newCharset) {
	if (str != null) {
	    // 用默认字符编码解码字符串。
	    byte[] bs = str.getBytes();
	    // 用新的字符编码生成字符串
	    try {
		return new String(bs, newCharset);
	    } catch (UnsupportedEncodingException e) {
		e.printStackTrace();
	    }
	}
	return null;
    }

    /**
     * 获取百度搜索结果数 获取如下文本并解析数字： 百度为您找到相关结果约13,200个
     * 
     * @param document 文档
     * @return 结果数
     */
    private int getBaiduSearchResultCount(Document document) {
	String cssQuery = "html body div div div div.nums";
	logger.debug("total cssQuery: " + cssQuery);
	Element totalElement = document.select(cssQuery).first();
	String totalText = totalElement.text();
	logger.info("搜索结果文本：" + totalText);

	String regEx = "[^0-9]";
	Pattern pattern = Pattern.compile(regEx);
	Matcher matcher = pattern.matcher(totalText);
	totalText = matcher.replaceAll("");
	int total = Integer.parseInt(totalText);
	logger.info("搜索结果数：" + total);
	return total;
    }

    public Map<String, ThreeTuple<String, String, String>> getLinkText(
	    List<TwoTuple<String, String>> links) {
	Map<String, ThreeTuple<String, String, String>> linkContext = new HashMap<>();
	List<String> pages = new ArrayList<>();
	Document doc = null;
	for (TwoTuple<String, String> link : links) {
	    // linkInfo三个属性对应链接文本，页面标题，页面内容
	    ThreeTuple<String, String, String> linkInfo = new ThreeTuple<>();
	    // 设置链接文本
	    linkInfo.setFirst(changeCharset(link.getSecond(), "UTF-8"));
	    try {
		doc = Jsoup.connect(link.getFirst()).get();
		String title = doc.getElementsByTag("title").first().text();
		// 设置页面标题
		linkInfo.setSecond(changeCharset(title, "UTF-8"));
		// 设置页面内容
		linkInfo.setThird(changeCharset(doc.body().text(), "UTF-8"));
	    } catch (IOException e) {
		logger.error("Connection error: " + e.getMessage());
		continue;
	    }
	    if (doc != null) {
		linkContext.put(link.getFirst(), linkInfo);
	    }
	}
	return linkContext;
    }

    public List<Document> getLinkText2(List<String> links) {
	List<Document> pages = new ArrayList<>();
	Document doc = null;
	for (String link : links) {
	    try {
		doc = Jsoup.connect(link).timeout(5000).get();
	    } catch (IOException e) {
		logger.error("Connection error: " + e.getMessage());
	    }
	    if (doc != null) {
		pages.add(doc);
	    }
	}
	return pages;
    }

    // 获取前10条记录的页面字符串
    public Map<String, ThreeTuple<String, String, String>> getTopSearchInfo(String keyword) {
	List<TwoTuple<String, String>> links = search(keyword);
	return getLinkText(links);
    }

    // 获取前10条记录的页面字符串
    public List<Document> getTopSearchPage(String keyword) {
	List<String> links = getPage(keyword, 1);
	return getLinkText2(links);
    }

    private List<String> getPage(String keyword, int page) {
	List<String> result = new ArrayList<>();
	Connection conn = Jsoup.connect(searchUrl).data("wd", keyword, "pn",
		String.valueOf((page - 1) * pageSize));
	Document doc = null;
	try {
	    doc = conn.get();
	} catch (IOException e) {
	    logger.error("Connection error: " + e.getMessage());
	}
	int resultNum = getBaiduSearchResultCount(doc);
	int len = (resultNum < pageSize) ? resultNum : pageSize;
	for (int i = 0; i < len; i++) {
	    String titleCssQuery = "#" + (i + 1 + (page - 1) * pageSize);
	    Element element = doc.select(titleCssQuery).first();
	    String link = element.attr("mu");
	    String linkText = element.text();
	    if ("".equals(link) || link == null) {
		titleCssQuery = "html body div div div div#content_left div#"
			+ (i + 1 + (page - 1) * pageSize) + ".result.c-container h3.t a";
		element = doc.select(titleCssQuery).first();
		link = element.attr("abs:href");
		linkText = element.text();
	    }
	    if (!"".equals(link) && link != null) {
		result.add(link);
	    }
	}
	return result;
    }

    /**
     * 获取该关键字的记录条数
     * 
     * @param key
     * @return
     */
    public long getSearchResultNum(String keyword) {
	Connection conn = Jsoup.connect(searchUrl).data("wd", keyword);
	Document doc = null;
	try {
	    doc = conn.get();
	} catch (IOException e) {
	    logger.error("Connection error: " + e.getMessage());
	    return 1000;
	}
	return getResultCount(doc);
    }

    private long getResultCount(Document document) {
	String cssQuery = "html body div div div div.nums";
	logger.debug("total cssQuery: " + cssQuery);
	Element totalElement = document.select(cssQuery).first();
	String totalText = totalElement.text();
	logger.info("搜索结果文本：" + totalText);

	String regEx = "[^0-9]";
	Pattern pattern = Pattern.compile(regEx);
	Matcher matcher = pattern.matcher(totalText);
	totalText = matcher.replaceAll("");
	long total = Long.parseLong(totalText);
	logger.info("搜索结果数：" + total);
	return total;
    }

    public static void main(String[] args) {
	BaiduSearcher searcher = new BaiduSearcher();
	Map<String, ThreeTuple<String, String, String>> pages = searcher.getTopSearchInfo("招聘");
	for (Entry<String, ThreeTuple<String, String, String>> string : pages.entrySet()) {
	    System.out.println(string);
	}
    }

}