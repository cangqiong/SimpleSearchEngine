package com.bigdata.downloader;

import static org.jsoup.Jsoup.parse;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.httpclient.HttpStatus;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.util.TwoTuple;

/**
 * 下载器，使用HttpClient下载页面
 *
 */
public class DownLoader {

    private static Logger logger = LoggerFactory.getLogger(DownLoader.class);

    // 请求参数
    private static RequestConfig requestConfig = RequestConfig.custom()
	    .setCookieSpec(CookieSpecs.STANDARD_STRICT).setSocketTimeout(3000)
	    .setConnectTimeout(3000).build();

    /**
     * 下载页面
     * 
     * @param url
     * @return
     */
    public static String download(String url) {
	if (url == null) {
	    return null;
	}
	// 设置超时连接
	HttpGet httpGet = new HttpGet(url);
	httpGet.setConfig(requestConfig);
	httpGet.addHeader("Content-Type", "text/html;charset=UTF-8");

	// 获取连接
	CloseableHttpClient httpClient = PoolManager.getHttpClient();
	CloseableHttpResponse response = null;
	try {
	    response = httpClient.execute(httpGet);
	    // 判断访问的状态码
	    int statusCode = response.getStatusLine().getStatusCode();
	    if (statusCode == HttpStatus.SC_OK) {
		HttpEntity entity = response.getEntity();
		if (entity != null) {
		    String charset = "utf-8";
		    Charset c = ContentType.getOrDefault(entity).getCharset();
		    String html = new String(EntityUtils.toString(entity));
		    if (c != null) {
			return html;
		    }
		    if (html.contains("charset=gb2312")) {
			charset = "gb2312";
		    }
		    if (html.contains("charset=gbk")) {
			charset = "gbk";
		    }
		    html = new String(html.getBytes("ISO-8859-1"), charset);
		    return html;
		}
	    }
	} catch (IOException e) {
	    logger.error("Download " + url + " error: " + e.getMessage());
	    return null;
	}
	return null;
    }

    /**
     * 获得下载页面的文档对象
     * 
     * @param url
     * @return
     */
    public static Document downloadPage(String url) {
	String page = download(url);
	Document doc = null;
	if (page != null) {
	    doc = Jsoup.parse(page, url);
	}
	return doc;
    }

    /**
     * 获取a标签的链接
     * 
     * @param html HTML字符串
     * @return
     */
    public static TwoTuple<String, Map<String, String>> getHtmlInfo(String html) {
	TwoTuple<String, Map<String, String>> htmlInfo = new TwoTuple<>();

	Map<String, String> pageInfo = new HashMap<String, String>();
	Document document = parse(html);
	// 获取纯文本
	htmlInfo.setFirst(document.text());

	// 获取链接
	Elements links = document.select("a[href]");
	for (Element link : links) {
	    String temp = transformLink(link.attr("abs:href"));
	    if (temp != null) {
		pageInfo.put(temp, link.text());
	    }
	}
	htmlInfo.setSecond(pageInfo);
	return htmlInfo;
    }

    /**
     * 将链接的无效消息去除，防止重复连接
     * 
     * @param url 原始链接
     * @return 有效链接
     */
    public static String transformLink(String url) {
	if (url == null || "".equals(url.trim())) {
	    return null;
	}
	if (url.indexOf("#") > 0) {
	    return url.substring(0, url.indexOf("#"));
	}
	return url;
    }

    public static void main(String[] args) {
	String page = download("http://www.baidu.com/");
	System.out.println(page);
    }
}
