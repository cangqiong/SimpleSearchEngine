package com.bigdata.downloader.handler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * 操作主题配置文件
 * 
 * @author Cang
 *
 */
@Getter
@Setter
@ToString
public class SubjectConfig {
    private String topicName;
    private double threshold;
    private List<String> keywords;
    private List<String> urlSeeds = new ArrayList<String>();

    /**
     * 加载配置文件
     * 
     * @return
     */
    public Document loadConfigFile() {
	// 实例化一个文档构建器工厂
	DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
	try {
	    // 通过文档构建器工厂获取一个文档构建器
	    DocumentBuilder db = dbf.newDocumentBuilder();
	    // 通过文档通过文档构建器构建一个文档实例
	    InputStream in = this.getClass().getClassLoader().getResourceAsStream("subject.xml");
	    Document doc = db.parse(in);
	    return doc;
	} catch (ParserConfigurationException ex) {
	    ex.printStackTrace();
	} catch (IOException ex) {
	    ex.printStackTrace();
	} catch (SAXException ex) {
	    ex.printStackTrace();
	}
	return null;
    }

    /**
     * 解析配置文件
     */
    public SubjectConfig config() {
	Document doc = loadConfigFile();
	// 获取“theme” 的节点
	NodeList nl = doc.getElementsByTagName("theme");
	Node theme = nl.item(0);
	NodeList nl2 = theme.getChildNodes();
	int len1 = nl2.getLength();
	for (int i = 0; i < len1; i++) {
	    Node n2 = nl2.item(i);
	    // 还是因为上面的原因，故此要处判断当 n2 节点有子节点的时才输出。
	    if (n2.hasChildNodes()) {
		if ("name".equals(n2.getNodeName())) {
		    topicName = n2.getFirstChild().getNodeValue();
		}
		if ("threshold".equals(n2.getNodeName())) {
		    {
			threshold = Double.parseDouble(n2.getFirstChild().getNodeValue());
		    }
		}
		if ("keywords".equals(n2.getNodeName())) {
		    String words = n2.getFirstChild().getNodeValue();
		    keywords = Arrays.asList(words.split(","));
		}
		if ("urlseed".equals(n2.getNodeName())) {
		    {
			urlSeeds.add(n2.getFirstChild().getNodeValue());
		    }
		}
	    }
	}
	return this;
    }

    public static void main(String[] args) {
	SubjectConfig config = new SubjectConfig().config();
	System.out.println(config);
    }
}
