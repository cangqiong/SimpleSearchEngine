package com.bigdata.downloader.spitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;

import org.ansj.app.keyword.KeyWordComputer;
import org.ansj.app.keyword.Keyword;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.downloader.DownLoader;
import com.bigdata.downloader.PriorityURL;
import com.bigdata.downloader.SimpleBloomFilter;
import com.bigdata.downloader.handler.CosineSimilarity;
import com.bigdata.downloader.handler.PageHandler;
import com.bigdata.downloader.searcher.BaiduSearcher;

/**
 * 爬虫主类
 * 
 * @author Cang
 *
 */
public class Crawler {
    private static final Logger logger = LoggerFactory.getLogger(Crawler.class);
    // 等待处理的网页
    private PriorityBlockingQueue<PriorityURL> waitforHandling = new PriorityBlockingQueue<PriorityURL>();
    private SimpleBloomFilter bloomFilter = new SimpleBloomFilter();
    private Map<String, Keyword> topicKeywordsMap = new TreeMap<>();
    private List<Keyword> topicKeywords;
    private static List<String> stopWords = new LinkedList<>();
    private String topicName;
    private String startUrl;
    // 阀值
    private double threshold = 0.6;

    // 加载停词表
    static {
	// 加载中文停词
	InputStream is = Crawler.class.getClassLoader().getResourceAsStream(
		"stopwords/stop_chinese_words.dic");
	try {
	    BufferedReader bf = new BufferedReader(new InputStreamReader(is, "UTF-8"));
	    String stopword = null;
	    while ((stopword = bf.readLine()) != null) {
		stopword = stopword.trim();
		stopWords.add(stopword);
	    }
	} catch (IOException e) {
	    logger.error(e.getMessage());
	}
	// 加载英文
	is = Crawler.class.getClassLoader().getResourceAsStream("stopwords/stop_english_words.dic");
	try {
	    BufferedReader bf = new BufferedReader(new InputStreamReader(is, "UTF-8"));
	    String stopword = null;
	    while ((stopword = bf.readLine()) != null) {
		stopword = stopword.trim();
		stopWords.add(stopword);
	    }
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

    public Crawler(String topicName, String startUrl) {
	this.topicName = topicName;
	this.startUrl = startUrl;
    }

    /**
     * 获取主题关键字（5-10）
     * 
     * @param pages 多个页面
     */
    public void getTopicKeyWords(List<Document> pages) {
	List<Keyword> allKeyWords = new ArrayList<>();
	for (Document page : pages) {
	    allKeyWords.addAll(getKeyWords(page, 20));
	}
	Map<String, Keyword> tempKeyWords = new TreeMap<>();
	Keyword temp = null;
	for (Keyword keyword : allKeyWords) {
	    if (stopWords.contains(keyword.getName())) {
		continue;
	    }
	    if ((temp = tempKeyWords.get(keyword.getName())) != null) {
		temp.setScore(temp.getScore() + keyword.getScore());
	    } else {
		tempKeyWords.put(keyword.getName(), keyword);
	    }
	}
	List<Keyword> result = filterData(tempKeyWords.values(), pages.size());
	Collections.sort(result);
	if (result.size() > 10) {
	    topicKeywords = result.subList(0, 10);
	} else {
	    topicKeywords = result;
	}
	for (Keyword keyword : result) {
	    topicKeywordsMap.put(keyword.getName(), keyword);
	}
    }

    /**
     * 过滤停用词
     * 
     * @param keywords 关键词列表
     * @return
     */
    public List<Keyword> filterData(Collection<Keyword> keywords, int pageNum) {
	List<Keyword> result = new ArrayList<>();
	for (Keyword keyword : keywords) {
	    if ("".equals(keyword.getName().trim())
		    || keyword.getName().matches("[\\S\\s]*\\d[\\S\\s]*")) {
		continue;
	    }
	    keyword.setScore(keyword.getScore() / pageNum);
	    result.add(keyword);
	}
	return result;
    }

    /**
     * 获取单独页面的关键词
     * 
     * @param page
     * @return keyWords 关键词列表（20）
     */
    public List<Keyword> getKeyWords(Document page, int num) {
	KeyWordComputer kwc = new KeyWordComputer(num);
	List<Keyword> result = kwc.computeArticleTfidf(page.title(), page.body().text());
	return result;
    }

    /**
     * 初始化
     */
    private void initialize() {
	// GoogleSercher searcher = new GoogleSercher();
	BaiduSearcher searcher = new BaiduSearcher();
	List<Document> pages = searcher.getTopSearchPage(topicName);
	getTopicKeyWords(pages);
	System.out.println("Topic keywords!");
	for (Keyword keyword : topicKeywords) {
	    System.out.println(keyword);
	}
	System.out.println("Add start page!");
	bloomFilter.add(startUrl);
	Document doc = DownLoader.downloadPage(startUrl);
	List<String> links = PageHandler.extraAllLinks(doc);
	for (String link : links) {
	    bloomFilter.add(link);
	    waitforHandling.add(new PriorityURL(link, 1));
	}
	System.out.println("End the initialize!");
    }

    class Task extends Thread {
	int number;

	Task(int number) {
	    this.number = number;
	}

	public void run() {
	    search();
	}
    }

    /**
     * 多线程运行程序
     */
    private void parallelhandle() {
	ExecutorService exec = Executors.newFixedThreadPool(2);
	for (int i = 0; i < 2; i++) {
	    exec.execute(new Task(i));
	}
    }

    /**
     * 分析链接
     */
    private void analysisLinks(List<String> links, double weghit) {
	Document doc = null;
	for (String link : links) {
	    if (bloomFilter.contains(link)) {
		continue;
	    }
	    bloomFilter.add(link);
	    doc = DownLoader.downloadPage(link);
	    if (doc == null) {
		continue;
	    }
	    List<Keyword> tempKeywords = getKeyWords(doc, 30);
	    Map<String, Keyword> pageVector = new TreeMap<>();
	    for (Keyword keyword : tempKeywords) {
		if (stopWords.contains(keyword.getName())) {
		    continue;
		}
		if (topicKeywordsMap.containsKey(keyword.getName())) {
		    pageVector.put(keyword.getName(), keyword);
		}
	    }
	    double result = countCorrelation(pageVector);
	    if (result > threshold) {
		System.out.println("The page simlation is " + result);
		System.out.println(doc.body().text());
		waitforHandling.add(new PriorityURL(link, result));
		PageHandler.storagePage(doc.html());
	    } else {
		System.out.println("The link is wrong!");
	    }
	}
    }

    /**
     * 计算主题相关度
     * 
     * @param links
     * @param weghit
     */
    private double countCorrelation(Map<String, Keyword> pageVector) {
	return CosineSimilarity.count(topicKeywordsMap, pageVector);
    }

    public void search() {
	while (!waitforHandling.isEmpty()) {
	    PriorityURL url = waitforHandling.poll();
	    System.out.println("Get url is :" + url.getUrl());
	    Document doc = DownLoader.downloadPage(url.getUrl());
	    if (doc != null) {
		List<String> links = PageHandler.extraAllLinks(doc);
		analysisLinks(links, url.getPriority());
	    }
	}
    }

    public static void main(String[] args) throws IOException {
	Crawler crawler = new Crawler("招聘", "http://www.zhaopin.com");
	crawler.initialize();
	crawler.parallelhandle();
    }
}