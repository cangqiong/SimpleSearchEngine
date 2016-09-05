package com.bigdata.downloader.spitter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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
public class HbaseSpitter extends Configured implements Tool {

    public static class SpitterMapper extends TableMapper<Text, IntWritable> {
	private static IntWritable one = new IntWritable(1);
	private static String family = "url";
	private static String qualifier = "adress";

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
		throws IOException, InterruptedException {
	    String url = Bytes.toString(value.getValue(Bytes.toBytes(family),
		    Bytes.toBytes(qualifier)));
	    System.out.println("Delete url: " + Bytes.toString(value.getRow()));
	    // 删除已经访问的链接
	    UrlPriorityQueue.deleteVisitedUrl(value.getRow());
	    System.out.println("Mapper consumed: " + url);
	    context.write(new Text(url), one);
	}
    }

    public static class SpitterReducer extends
	    TableReducer<Text, IntWritable, ImmutableBytesWritable> {
	// private static DownLoader downloader = new DownLoader();
	private static String family1 = "contents";
	private static String family2 = "anchors";
	private static String qualifier1 = "html";
	private static String qualifier2 = "text";

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
	    byte[] keyBytes = Bytes.toBytes(StringTools.reverseUrl(key.toString()));
	    // 下载页面并获取页面信息
	    String page = DownLoader.download(key.toString());
	    if (page != null) {
		TwoTuple<String, Map<String, String>> pageInfo = DownLoader.getHtmlInfo(page);
		System.out.println("The real url is: " + key.toString());
		
		// 添加一行记录，每一个单词作为行键
		Put put = new Put(keyBytes);
		put.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qualifier1),
			Bytes.toBytes(page));
		put.addColumn(Bytes.toBytes(family1), Bytes.toBytes(qualifier2),
			Bytes.toBytes(pageInfo.getFirst()));

		// 添加未访问链接
		UrlPriorityQueue.addLinks(pageInfo.getSecond());

		// 为当前url添加链接信息
//		for (Entry<String, String> linkInfo : pageInfo.getSecond().entrySet()) {
//		    put.addColumn(Bytes.toBytes(family2), Bytes.toBytes(linkInfo.getKey()),
//			    Bytes.toBytes(linkInfo.getValue()));
//		}
		System.out.println("Add page :" + Bytes.toString(keyBytes));
		context.write(new ImmutableBytesWritable(keyBytes), put);
	    }
//	    context.write(new ImmutableBytesWritable(keyBytes), new Put(keyBytes));
	}
    }

    @Override
    public int run(String[] args) throws Exception {

	Configuration conf = HBaseConfiguration.create();
	conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	Job job = Job.getInstance(conf, "HbaseSpitter");
	job.setJarByClass(HbaseSpitter.class);
	job.setNumReduceTasks(2);

	Scan scan = new Scan();
	// 是否缓存块
	scan.setCacheBlocks(false);
	// 每次从服务器端读取的行数，默认为配置文件中设置的值
	scan.setCaching(20);
	scan.setBatch(3);

	// 指定Mapper读取的表为url_info
	TableMapReduceUtil.initTableMapperJob("url_priority_queue", scan, SpitterMapper.class,
		Text.class, IntWritable.class, job);
	// 指定Reducer写入的表为url_info
	TableMapReduceUtil.initTableReducerJob("url_info", SpitterReducer.class, job);
	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	DateFormat format = new SimpleDateFormat("yyyyMMddhhmmss");
	// while (true) {
	ToolRunner.run(new Configuration(), new HbaseSpitter(), args);
	System.out.println(format.format(new Date()));
	// }
    }
}
