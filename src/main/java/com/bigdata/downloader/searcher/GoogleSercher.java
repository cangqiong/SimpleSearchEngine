package com.bigdata.downloader.searcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleSercher {
    private static final Logger logger = LoggerFactory.getLogger(GoogleSercher.class);

    private String searchUrl = "http://gfss.cc.wallpai.com/search";
    private int pageSize = 10;

    public List<String> search(String keyword, int pageNum) {
	List<String> links = new ArrayList<>();
	Connection conn = Jsoup
		.connect(searchUrl)
		.data("q", keyword, "start", String.valueOf((pageNum - 1) * pageSize), "sa", "N")
		.timeout(5000)
		.userAgent(
			"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.64 Safari/537.31");
	// 获取页面
	Document doc = null;
	try {
	    doc = conn.get();
	} catch (IOException e) {
	    System.out.println(e);
	    logger.error("Get page error! :" + e.getMessage());
	}
	String cssQuery = null;
	cssQuery = "#rso > div.g > div > div > h3 > a";
	Element ele = doc.select(cssQuery).first();
	if (ele != null) {
	    links.add(ele.attr("abs:href"));
	}
	cssQuery = "#rso > div.srg > div > div > h3 > a";
	Elements els = doc.select(cssQuery);
	for (Element element : els) {
	    String link = element.attr("abs:href");
	    if (!"".equals(link) && link != null) {
		links.add(link);
	    }
	}
	cssQuery = "#rso > div._NId > div > div > div > h3 > a";

	Elements els2 = doc.select(cssQuery);
	for (Element element : els2) {
	    String link = element.attr("abs:href");
	    if (!"".equals(link) && link != null) {
		links.add(link);
	    }
	}
	return links;
    }

    /**
     * 获取谷歌搜索结果数 获取如下文本并解析数字
     * 
     * @param document 文档
     * @return 结果数
     */
    private long getResultCount(Document document) {
	String cssQuery = "#resultStats";
	Element totalElement = document.select(cssQuery).first();
	String totalText = totalElement.text();
	String regEx = "[^0-9]";
	Pattern pattern = Pattern.compile(regEx);
	Matcher matcher = pattern.matcher(totalText);
	totalText = matcher.replaceAll("");
	long total = Long.parseLong(totalText);
	System.out.println("搜索结果数：" + total);
	return total;
    }

    public List<Document> getLinkText(List<String> links) {
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

    /**
     * 获取搜索首页的信息
     * 
     * @param keyword
     * @return
     */
    public List<Document> getTopSearchInfo(String keyword) {
	List<String> links = search(keyword, 1);
	return getLinkText(links);
    }

    public static void main(String[] args) {
	GoogleSercher searcher = new GoogleSercher();
	List<Document> pages = searcher.getTopSearchInfo("招聘");
	System.out.println("Page size is:" + pages.size());
	for (Document string : pages) {
	    System.out.println(string);
	}
    }
}