package com.bigdata.downloader.spitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.ansj.app.keyword.Keyword;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.jsoup.nodes.Document;

import com.bigdata.downloader.DownLoader;
import com.bigdata.downloader.UrlPriorityQueue;
import com.bigdata.downloader.handler.PageHandler;
import com.bigdata.downloader.handler.SubjectConfig;
import com.bigdata.downloader.handler.TopicAnalysis;

/**
 * 基于Hadoop的主题爬虫
 * 
 * @author Cang
 *
 */
public class SimpleTopicSpitter extends Configured implements Tool {

    private static TopicAnalysis analysis;
    private static Map<String, Keyword> topicKeywordsMap = new TreeMap<>();
    private static UrlPriorityQueue priorityQueue;

    public static class SimpleTopicMapper extends TableMapper<Text, IntWritable> {
	private static IntWritable one = new IntWritable(1);
	private static int count = 0;

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
		throws IOException, InterruptedException {
	    String url = Bytes.toString(
		    value.getValue(Bytes.toBytes("url"), Bytes.toBytes("adress"))).toLowerCase();
	    double weghit = Bytes.toDouble(value.getRow());
	    // 删除已经访问的链接
	    priorityQueue.transformVisitedUrl(Bytes.toString(value.getRow()));
	    // System.out.println(count++);
	    // System.out.println(Bytes.toString(value.getRow()));
	    context.write(new Text(url + "," + weghit), one);
	}
    }

    public static class SimpleTopicReducer extends
	    TableReducer<Text, IntWritable, ImmutableBytesWritable> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
	    String[] strArr = key.toString().split(",");
	    String url = strArr[0];
	    double initWeghit = Double.parseDouble(strArr[1]);
	    Document page = DownLoader.downloadPage(url);
	    if (page != null) {
		// 计算余弦相似度
		double weight = analysis.countSimilarity(page, topicKeywordsMap, initWeghit);
		if (weight > analysis.getThreshold()) {
		    System.out.println("Add page: " + url);
		    priorityQueue.savePageInfo(url, page, weight);
		}
		List<String> links = PageHandler.extraAllLinks(page);
		for (String link : links) {
		    if (priorityQueue.contain(link)) {
			continue;
		    }
		    priorityQueue.addLink(link, weight);
		}
	    }
	    // // 下载页面并获取页面信息
	    // String page = DownLoader.download(key.toString());
	    //
	    // if (page != null) {
	    // TwoTuple<String, Map<String, String>> pageInfo =
	    // DownLoader.getHtmlInfo(page);
	    // System.out.println("Start add link :" +
	    // System.currentTimeMillis());
	    // // 添加未访问链接
	    // UrlPriorityQueue.addLinks(pageInfo.getSecond());
	    // System.out.println("End add link :" +
	    // System.currentTimeMillis());
	    // // 添加页面信息
	    // byte[] keyBytes = Bytes.toBytes(StringTools.reverseUrl(url));
	    // // 添加一行记录，每一个单词作为行键
	    // Put put = new Put(keyBytes);
	    // put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("html"),
	    // Bytes.toBytes(page));
	    // put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("text"),
	    // Bytes.toBytes(pageInfo.getFirst()));
	    // // 为当前url添加链接信息
	    // for (Entry<String, String> linkInfo :
	    // pageInfo.getSecond().entrySet()) {
	    // put.addColumn(Bytes.toBytes("anchors"),
	    // Bytes.toBytes(linkInfo.getKey()),
	    // Bytes.toBytes(linkInfo.getValue()));
	    // }
	    // UrlPriorityQueue.addPageInfo(put);
	    // System.out.println("Add page :" + url);
	    // }
	    // context.write(new ImmutableBytesWritable(keyBytes), new
	    // Put(keyBytes));
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	// 加载主题爬虫配置信息
	SubjectConfig config = new SubjectConfig().config();
	// 训练得出主题关键词
	analysis = new TopicAnalysis(config);
	List<Keyword> topicKeywords = SubjectSpitter.loadKeywords();
	for (Keyword keyword : topicKeywords) {
	    topicKeywordsMap.put(keyword.getName(), keyword);
	}
	priorityQueue = new UrlPriorityQueue(analysis.getTopicName());
	// 根据主题创建数据库
	priorityQueue.createTable();
	for (String startUrl : analysis.getUrlSeeds()) {
	    if (priorityQueue.contain(startUrl)) {
		continue;
	    }
	    priorityQueue.addLink(startUrl, 1);
	}
	Configuration conf = HBaseConfiguration.create();
	conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	Job job = Job.getInstance(conf, "SimpleSpitter");
	job.setJarByClass(SimpleTopicSpitter.class);

	Scan scan = new Scan();
	// 是否缓存块
	scan.setCacheBlocks(false);
	scan.setStopRow(Bytes.toBytes("3971320151127095743842"));
	// 每次从服务器端读取的行数，默认为配置文件中设置的值
	// scan.setCaching(10);
	// 多条件过滤
	FilterList filterList = new FilterList();
	Filter filter = new SingleColumnValueFilter(Bytes.toBytes("url"), Bytes.toBytes("visited"),
		CompareOp.EQUAL, Bytes.toBytes("false"));
	filterList.addFilter(filter);
	scan.setFilter(filterList);

	// 指定Mapper读取的表为url_info
	TableMapReduceUtil.initTableMapperJob(priorityQueue.getUrlQueue(), scan,
		SimpleTopicMapper.class, Text.class, IntWritable.class, job);
	TableMapReduceUtil.initTableReducerJob("url_info", SimpleTopicReducer.class, job);
	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	ToolRunner.run(new Configuration(), new SimpleTopicSpitter(), args);
    }
}
