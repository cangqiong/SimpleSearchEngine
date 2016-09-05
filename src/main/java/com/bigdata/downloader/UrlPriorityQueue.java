package com.bigdata.downloader;

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import lombok.Getter;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.downloader.robots.Robots;
import com.bigdata.util.HBaseUtils;
import com.bigdata.util.MD5Util;
import com.bigdata.util.StringTools;

/**
 * url优先队列
 * 
 * @author Cang
 *
 */
@Getter
public class UrlPriorityQueue {

    private static Logger logger = LoggerFactory.getLogger(UrlPriorityQueue.class);
    // 随机生成后缀名
    private DateFormat formatter = new SimpleDateFormat("yyyyMMddhhmmssSSS");

    // url访问链接表
    private String urlQueue;
    // 网页信息表
    private String ContentTable = "url_info";
    // 主题名称
    private String topic;

    public UrlPriorityQueue(String topicName) {
	this.topic = topicName;
	this.urlQueue = "url_queue_" + MD5Util.MD5(topic);
    }

    /**
     * 判断该链接是否能够访问
     * 
     * @param urlStr
     * @return
     */
    public static boolean accept(String urlStr) {
	// System.out.println("Start accept:" + System.currentTimeMillis());
	try {
	    URL url = new URL(urlStr);
	    // System.out.println("Start filter:" + System.currentTimeMillis());
	    // 1) 页面是否为HTML格式
	    if (Robots.isRobotAllowed(url)) {
		// System.out.println("End " + url + " :" +
		// System.currentTimeMillis());
	    }
	} catch (MalformedURLException e) {
	    logger.error("Link transform to URL error!");
	    return false;
	}
	return false;
    }

    /**
     * 判断链接是否已经访问
     * 
     * @param startUrl
     * @return
     */
    public synchronized boolean contain(String url) {
	return HBaseUtils.checkColumnValueExist(urlQueue, "url", "adress", url);
    }

    /**
     * 添加需要访问的链接
     * 
     * @param url
     * @param initWeghit 初识权重，即父页面的权重
     */
    public void addLink(String url, double initWeghit) {
	int t = (int) (100000 * (1 - initWeghit));
	// 补零
	String weghit = String.format("%05d", t) + formatter.format(new Date());
	Put put = new Put(Bytes.toBytes(weghit));
	put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("adress"), Bytes.toBytes(url));
	put.addColumn(Bytes.toBytes("url"), Bytes.toBytes("visited"), Bytes.toBytes("false"));
	HBaseUtils.PutData(urlQueue, put);
    }

    /**
     * 删除已经访问的链接
     * 
     * @param row
     */
    public synchronized void  transformVisitedUrl(String row) {
	HBaseUtils.PutData(urlQueue, Bytes.toBytes(row), "url", "visited", "true");
    }

    /**
     * 保存页面信息
     * 
     * @param url
     * @param doc
     * @param weghit
     */
    public void savePageInfo(String url, Document doc, double weghit) {
	Put put = new Put(Bytes.toBytes(StringTools.reverseUrl(url)));
	put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("html"), Bytes.toBytes(doc.html()));
	put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("title"), Bytes.toBytes(doc.title()));
	put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("text"), Bytes.toBytes(doc.text()));
	put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes(topic), Bytes.toBytes(weghit));
	HBaseUtils.PutData(ContentTable, put);

    }

    /**
     * 将最高优先级的链接输出
     * 
     * @return
     */
    public synchronized PriorityURL poll() {
	Scan scan = new Scan();
	// 是否缓存块
	scan.setCacheBlocks(false);
	// 每次从服务器端读取的行数，默认为配置文件中设置的值
	scan.setCaching(1);
	Filter filter = new SingleColumnValueFilter(Bytes.toBytes("url"), Bytes.toBytes("visited"),
		CompareOp.EQUAL, Bytes.toBytes("false"));
	scan.setFilter(filter);
	Cell cell = HBaseUtils.firstCloumnData(urlQueue, scan, "url", "adress");
	String url = null;
	String row = null;
	try {
	    url = new String(CellUtil.cloneValue(cell), "UTF-8");
	    row = new String(CellUtil.cloneRow(cell), "UTF-8");
	} catch (UnsupportedEncodingException e) {
	    e.printStackTrace();
	}
	double weghit = StringTools.getPriority(row);
	transformVisitedUrl(row);
	PriorityURL priorityURL = new PriorityURL(url, weghit);
	return priorityURL;
    }

    /**
     * 创建所需的数据表
     */
    public void createTable() {
	HBaseUtils.CreateTable(urlQueue, new String[] { "url" });
	HBaseUtils.CreateTable(ContentTable, new String[] { "contents", "anchors", "topic" });
    }

}
