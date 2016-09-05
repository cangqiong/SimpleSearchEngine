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

public class SogouSearcher {
    private static final Logger logger = LoggerFactory.getLogger(SogouSearcher.class);

    private String searchUrl = "http://www.sogou.com/sogou";
    private int pageSize = 10;
    private String keyword;

    public SogouSearcher() {
    }

    public SogouSearcher(String keyword) {
	this.keyword = keyword;
    }

    public void search(String keyword) {
	search(keyword, 1);
    }

    public List<String> search(String keyword, int page) {
	List<String> links = new ArrayList<>();
	Connection conn = Jsoup.connect(searchUrl).data("query", keyword, "page",
		String.valueOf((page - 1) * pageSize));
	Document doc = null;
	try {
	    doc = conn.get();
	} catch (IOException e) {
	    logger.error("Connection error: " + e.getMessage());
	}
	int resultNum = getSogouSearchResultCount(doc);
	int len = (resultNum < pageSize) ? resultNum : pageSize;
	System.out.println(doc);
	String titleCssQuery = "div.docid";
	for (int i = 0; i < len; i++) {
	    Elements els =  doc.select(titleCssQuery);
	    Element element = els.get(i);
	    String link = element.select("h3.pt > a").first().attr("abs:href");
	    if (!"".equals(link) && link != null) {
		links.add(link);
	    }
	}
	return links;
    }

    /**
     * 获取百度搜索结果数 获取如下文本并解析数字： 百度为您找到相关结果约13,200个
     * 
     * @param document 文档
     * @return 结果数
     */
    private int getSogouSearchResultCount(Document document) {
	String cssQuery = "resnum#scd_num";
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

    public List<String> getLinkText(List<String> links) {
	List<String> pages = new ArrayList<>();
	Document doc = null;
	for (String link : links) {
	    try {
		doc = Jsoup.connect(link).get();
	    } catch (IOException e) {
		logger.error("Connection error: " + e.getMessage());
	    }
	    if (doc != null) {
		pages.add(doc.text());
	    }
	}
	return pages;
    }

    public static void main(String[] args) {
	SogouSearcher searcher = new SogouSearcher();
	List<String> links = searcher.search("招聘", 1);
	List<String> pages = searcher.getLinkText(links);
	for (String string : pages) {
	    System.out.println(string);
	}
    }
}