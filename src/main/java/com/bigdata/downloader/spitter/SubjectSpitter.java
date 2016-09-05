package com.bigdata.downloader.spitter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ansj.app.keyword.Keyword;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.downloader.DownLoader;
import com.bigdata.downloader.PriorityURL;
import com.bigdata.downloader.UrlPriorityQueue;
import com.bigdata.downloader.handler.PageHandler;
import com.bigdata.downloader.handler.SubjectConfig;
import com.bigdata.downloader.handler.TopicAnalysis;
import com.bigdata.util.FileUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * 主题爬虫主类
 * 
 * @author Cang
 *
 */
public class SubjectSpitter {
    private static final Logger logger = LoggerFactory.getLogger(SubjectSpitter.class);
    // 等待处理的网页
    private UrlPriorityQueue priorityQueue;
    private Map<String, Keyword> topicKeywordsMap;
    private List<Keyword> topicKeywords;
    private TopicAnalysis analysis;
    private String topicName;
    private List<String> startUrls;
    // 阀值
    private double threshold = 0.6;

    public SubjectSpitter(TopicAnalysis analysis, List<Keyword> topicKeywords) {
	this.analysis = analysis;
	this.topicName = analysis.getTopicName();
	this.startUrls = analysis.getUrlSeeds();
	this.threshold = analysis.getThreshold();
	this.topicKeywords = topicKeywords;
    }

    /**
     * 初始化
     */
    private void initialize() {
	priorityQueue = new UrlPriorityQueue(topicName);
	topicKeywordsMap = getKeywordsMap(topicKeywords);
	System.out.println("Topic keywords!");
	for (Keyword keyword : topicKeywords) {
	    System.out.println(keyword);
	}
	System.out.println("Add start page!");
	// 根据主题创建数据库
	priorityQueue.createTable();
	for (String startUrl : startUrls) {
	    if (priorityQueue.contain(startUrl)) {
		continue;
	    }
	    priorityQueue.addLink(startUrl, 1);
	}

	System.out.println("End the initialize!");
    }

    /**
     * 多线程运行程序
     * 
     * @param threadNum
     */
    private void parallelhandle(int threadNum) {
	ExecutorService exec = Executors.newFixedThreadPool(threadNum);
	for (int i = 0; i < threadNum; i++) {
	    exec.execute(new Runnable() {
		@Override
		public void run() {
		    while (!Thread.interrupted()) {
			PriorityURL url = priorityQueue.poll();
			// System.out.println("Get url is :" + url.getUrl());
			Document doc = DownLoader.downloadPage(url.getUrl());
			if (doc != null) {
			    // 计算余弦相似度
			    double weight = analysis.countSimilarity(doc, topicKeywordsMap,
				    url.getPriority());
			    if (weight > threshold) {
				storagePage(url.getUrl(), doc, weight);
			    }
			    List<String> links = PageHandler.extraAllLinks(doc);
			    for (String link : links) {
				handleNewUrl(link, weight);
			    }
			}
		    }
		}
	    });
	}
	exec.shutdown();
    }

    /**
     * 获取关键词映射
     * 
     * @return
     * 
     */
    public Map<String, Keyword> getKeywordsMap(List<Keyword> result) {
	Map<String, Keyword> topicKeywordsMap = new TreeMap<>();
	for (Keyword keyword : result) {
	    topicKeywordsMap.put(keyword.getName(), keyword);
	}
	return topicKeywordsMap;
    }

    /**
     * 添加新链接
     * 
     * @param url
     * @param initWeghit
     * @return
     */
    public void handleNewUrl(String url, double initWeghit) {
	if (priorityQueue.contain(url)) {
	    return;
	}
	priorityQueue.addLink(url, initWeghit);
    }

    /**
     * 存储页面
     * 
     * @param url
     * @param doc
     * @param weghit
     */
    private void storagePage(String url, Document doc, double weghit) {
	System.out.println("Add page success! " + url);
	System.out.println(doc.text());
	priorityQueue.savePageInfo(url, doc, weghit);
    }

    /**
     * 加载关键词
     * 
     * @return
     */
    public static List<Keyword> loadKeywords() {
	String test = FileUtils.File2String("key.txt");
	System.out.println(test);
	Gson gson = new Gson();
	List<Keyword> keywords = new ArrayList<Keyword>();
	Type type = new TypeToken<ArrayList<Keyword>>() {
	}.getType();
	keywords = gson.fromJson(test, type);
	return keywords;
    }

    public static void main(String[] args) throws IOException {
	// 加载主题爬虫配置信息
	SubjectConfig config = new SubjectConfig().config();
	// 训练得出主题关键词
	TopicAnalysis analysis = new TopicAnalysis(config);
	List<Keyword> topicKeywords = loadKeywords();
	if (topicKeywords.isEmpty()) {
	    System.out.println("load ");
	    topicKeywords = analysis.trainTopic();
	}
	SubjectSpitter crawler = new SubjectSpitter(analysis, topicKeywords);
	crawler.initialize();
	crawler.parallelhandle(3);
    }
}