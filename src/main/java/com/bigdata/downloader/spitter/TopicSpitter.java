package com.bigdata.downloader.spitter;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bigdata.downloader.DownLoader;
import com.bigdata.downloader.UrlPriorityQueue;
import com.bigdata.util.StringTools;
import com.bigdata.util.TwoTuple;

/**
 * 基于Hadoop的主题爬虫
 * 
 * @author Cang
 *
 */
public class TopicSpitter extends Configured implements Tool {

    public static class TopicMapper extends TableMapper<Text, IntWritable> {
	private static IntWritable one = new IntWritable(1);
	private static String topic = "test";

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
		throws IOException, InterruptedException {
	    String url = Bytes.toString(
		    value.getValue(Bytes.toBytes("url"), Bytes.toBytes("adress"))).toLowerCase();
	    // 删除已经访问的链接
	    UrlPriorityQueue.transformVisitedUrl(value.getRow(), "topic", topic + "-visited",
		    "true");

	    // 下载页面并获取页面信息
	    String page = DownLoader.download(url);
	    System.out.println("Mapper consumed: " + url);
	    if (page != null) {
		TwoTuple<String, Map<String, String>> pageInfo = DownLoader.getHtmlInfo(page);
		System.out.println("Start add link :" + System.currentTimeMillis());
		// 添加未访问链接
		UrlPriorityQueue.addLinks(pageInfo.getSecond());
		System.out.println("End add link :" + System.currentTimeMillis());
		// 添加页面信息
		byte[] keyBytes = Bytes.toBytes(StringTools.reverseUrl(url));
		// 添加一行记录，每一个单词作为行键
		Put put = new Put(keyBytes);
		put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("html"), Bytes.toBytes(page));
		put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("text"),
			Bytes.toBytes(pageInfo.getFirst()));
		// 为当前url添加链接信息
		for (Entry<String, String> linkInfo : pageInfo.getSecond().entrySet()) {
		    put.addColumn(Bytes.toBytes("anchors"), Bytes.toBytes(linkInfo.getKey()),
			    Bytes.toBytes(linkInfo.getValue()));
		}
		UrlPriorityQueue.addPageInfo(put);
		System.out.println("Add page :" + url);
	    }
	    // context.write(new Text(url), one);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	// 主题关键词
	String topic = args[1];
	// 种子Url
	// String seedUlr = args[1];
	Configuration conf = HBaseConfiguration.create();
	conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	Job job = Job.getInstance(conf, "TopicSpitter");
	job.setJarByClass(TopicSpitter.class);
	// 去除reduce过程
	job.setNumReduceTasks(0);
	job.setMaxMapAttempts(15);
	Path out = new Path(args[1]);
	out.getFileSystem(conf).delete(out, true);
	FileOutputFormat.setOutputPath(job, out);

	Scan scan = new Scan();
	// 是否缓存块
	scan.setCacheBlocks(false);
	// 每次从服务器端读取的行数，默认为配置文件中设置的值
	scan.setCaching(10);

	Filter filter = new SingleColumnValueFilter(Bytes.toBytes("topic"), Bytes.toBytes(topic
		+ "-visited"), CompareOp.EQUAL, Bytes.toBytes("false"));
	scan.setFilter(filter);
	scan.setCaching(10);
	// 指定Mapper读取的表为url_info
	TableMapReduceUtil.initTableMapperJob("url_priority_queue", scan, TopicMapper.class,
		Text.class, IntWritable.class, job);
	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	String[] path = { "hdfs://10.21.71.132:9000/user/hadoop/output/topic", "test" };
	// while (true) {
	ToolRunner.run(new Configuration(), new TopicSpitter(), path);
	// }
    }
}
