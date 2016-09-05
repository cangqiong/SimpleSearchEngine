package com.bigdata.downloader.handler;

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

import lombok.Getter;

import org.ansj.app.keyword.KeyWordComputer;
import org.ansj.app.keyword.Keyword;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.downloader.searcher.BaiduSearcher;
import com.bigdata.downloader.spitter.Crawler;
import com.bigdata.util.FileUtils;
import com.google.gson.Gson;

/**
 * 主题分析
 * 
 * @author Cang
 *
 */
@Getter
public class TopicAnalysis {
    private static final Logger logger = LoggerFactory.getLogger(TopicAnalysis.class);
    public static List<String> stopWords = new LinkedList<>();
    private String topicName;
    private double threshold;
    private List<String> userKeywords;
    private List<String> urlSeeds;
    private List<Keyword> topicKeywords;

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

    public TopicAnalysis(SubjectConfig config) {
	this.topicName = config.getTopicName();
	this.urlSeeds = config.getUrlSeeds();
	this.threshold = config.getThreshold();
	this.userKeywords = config.getKeywords();
    }

    /**
     * 获取单独页面的关键词
     * 
     * @param page
     * @return keyWords 关键词列表（20）
     */
    public List<Keyword> getKeyWords(Document page, int num) {
	List<Keyword> result = new ArrayList<>();
	KeyWordComputer kwc = new KeyWordComputer(num);
	List<Keyword> keywords = kwc.computeArticleTfidf(page.title(), page.body().text());
	for (Keyword keyword : keywords) {
	    if (stopWords.contains(keyword.getName())) {
		continue;
	    }
	    result.add(keyword);
	}
	return result;
    }

    /**
     * 获取主题关键字（5-10）
     * 
     * @param pages 多个页面
     * @return
     */
    public List<Keyword> getTopicKeyWords(List<Document> pages) {
	List<Keyword> allKeyWords = new ArrayList<>();
	List<Keyword> topicKeywords = new ArrayList<>();
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
	return topicKeywords;
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
     * 计算文档与主题的相关度
     * 
     * @param doc
     * @param topicKeywordsMap
     * @param parentPriority 父页面的初始权重
     * @return
     */
    public double countSimilarity(Document doc, Map<String, Keyword> topicKeywordsMap,
	    double parentPriority) {

	Map<String, Keyword> pageVector = new TreeMap<>();
	// List<Keyword> tempKeywords = getKeyWords(doc, 30);
	// Map<String, Keyword> pageVector = new TreeMap<>();
	// for (Keyword keyword : tempKeywords) {
	// if (topicKeywordsMap.containsKey(keyword.getName())) {
	// pageVector.put(keyword.getName(), keyword);
	// }
	// }

	List<Term> terms = ToAnalysis.parse(doc.text());
	int count = 0;
	for (Term term : terms) {
	    String wordName = term.getName();
	    if (stopWords.contains(wordName)) {
		continue;
	    }
	    if (topicKeywordsMap.containsKey(wordName)) {
		if (pageVector.containsKey(wordName)) {
		    Keyword word = pageVector.get(wordName);
		    word.setScore(word.getScore() + 1);
		    pageVector.put(wordName, word);
		} else {
		    pageVector.put(wordName, new Keyword(wordName, 1));
		}
	    }
	    count++;
	}
	for (Keyword entry : pageVector.values()) {
	    entry.setScore(entry.getScore() / count);
	}

	double contentWeghit = CosineSimilarity.count(topicKeywordsMap, pageVector);
	double result = parentPriority * 0.2 + 0.8 * contentWeghit;
	return result;
    }

    /**
     * 获取主题关键词
     * 
     * @return
     * 
     * @return
     */
    public List<Keyword> trainTopic() {
	BaiduSearcher searcher = new BaiduSearcher();
	List<Document> pages = searcher.getTopSearchPage(topicName);
	List<Keyword> topicKeywords = getTopicKeyWords(pages);
	saveKeywords(topicKeywords);
	return topicKeywords;
    }

    /**
     * 保存主题关键词
     * 
     * @param topicKeywords
     * 
     * @return
     */
    public void saveKeywords(List<Keyword> topicKeywords) {
	Gson gson = new Gson();
	String text = gson.toJson(topicKeywords);
	FileUtils.save(text, "key.txt");
	System.out.println(text);
    }

    public static void main(String[] args) {
	// 加载主题爬虫配置信息
	SubjectConfig config = new SubjectConfig().config();
	TopicAnalysis analysis = new TopicAnalysis(config);
	List<Keyword> topicKeywords = analysis.trainTopic();
	System.out.println("Topic keywords!");
	for (Keyword keyword : topicKeywords) {
	    System.out.println(keyword);
	}
    }

}
