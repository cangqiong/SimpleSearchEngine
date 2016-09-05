package com.bigdata.index.parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 有词频等属性的倒排索引
 * 
 * @author Cang
 *
 */
public class InvertedIndex extends Configured implements Tool {

    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final IntWritable one = new IntWritable(1);
	private Set<String> stopWords;
	private URI[] localFiles;

	// 加载停词表
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
	    stopWords = new TreeSet<String>();
	    Configuration conf = context.getConfiguration();
	    // 加载缓冲文件
	    localFiles = context.getCacheFiles();
	    for (int i = 0; i < localFiles.length; i++) {
		URI uri = localFiles[i];
		String line;
		BufferedReader br = new BufferedReader(new FileReader(uri.getPath()));
		while ((line = br.readLine()) != null) {
		    StringTokenizer itr = new StringTokenizer(line);
		    while (itr.hasMoreTokens()) {
			stopWords.add(itr.nextToken());
		    }
		}
	    }
	}

	protected void map(Text key, IntWritable value, Context context) throws IOException,
		InterruptedException {
	    FileSplit fileSplit = (FileSplit) context.getInputSplit();
	    // 获取文件名
	    String fileName = fileSplit.getPath().getName();
	    String temp = new String();
	    String line = value.toString().toLowerCase();
	    StringTokenizer itr = new StringTokenizer(line);
	    while (itr.hasMoreTokens()) {
		temp = itr.nextToken();
		if (!stopWords.contains(temp)) {
		    Text word = new Text();
		    word.set(temp + "#" + fileName);
		    context.write(word, one);
		}
	    }
	}
    }

    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
	    int sum = 0;
	    for (IntWritable val : values) {
		sum += val.get();
	    }
	    result.set(sum);
	    context.write(key, result);
	}

    }

    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	    String term = new String();
	    term = key.toString().split("#")[0]; // (term, docid)=>term
	    return super.getPartition(new Text(term), value, numReduceTasks);
	}

    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Text word1 = new Text();
	private Text word2 = new Text();
	String temp = new String();
	static Text CurrentItem = new Text(" ");
	static List<String> postingList = new ArrayList<String>();

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
	    int sum = 0;
	    word1.set(key.toString().split("#")[0]);
	    temp = key.toString().split("#")[1];
	    for (IntWritable val : values) {
		sum += val.get();
	    }
	    word2.set("<" + temp + "," + sum + ">");
	    if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) {
		StringBuilder out = new StringBuilder();
		long count = 0;
		for (String p : postingList) {
		    out.append(p);
		    out.append(";");
		    count += Long.parseLong(p.substring(p.indexOf(",") + 1, p.indexOf(">")));
		}
		out.append("<total," + count + ">.");
		if (count > 0) {
		    context.write(CurrentItem, new Text(out.toString()));
		}
		postingList = new ArrayList<String>();
	    }

	    CurrentItem = new Text(word1);
	    postingList.add(word2.toString());
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, Text>.Context context)
		throws IOException, InterruptedException {
	    StringBuilder out = new StringBuilder();
	    long count = 0;
	    for (String p : postingList) {
		out.append(p);
		out.append(";");
		count += Long.parseLong(p.substring(p.indexOf(",") + 1, p.indexOf(">")));
	    }
	    out.append("<total," + count + ">.");
	    if (count > 0) {
		context.write(CurrentItem, new Text(out.toString()));
	    }
	}

    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = new Configuration(getConf());
	// conf.addResource("classpath:/hadoop/core-site.xml");
	// conf.addResource("classpath:/hadoop/hdfs-site.xml");
	// conf.addResource("classpath:/hadoop/mapred-site.xml");
	conf.set("fs.default.name", "hdfs://10.21.71.132:900");

	DistributedCache.addCacheFile(new Path(
		"hdfs://10.21.71.132:9000/user/hadoop/input/stopwords/1.txt").toUri(), conf);
	Job job = Job.getInstance(conf, "InvertedIndex");
	// 加入停用词

	// job.addCacheFile(new URI(
	// "hdfs://10.21.71.132:9000/user/hadoop/input/stopwords/1.txt"));
	// job.addCacheFile(new URI(
	// "hdfs://10.21.71.132:54310/user/hadoop/input/stopwords/stop_english_words.dic"));
	job.setJarByClass(InvertedIndex.class);
	job.setMapperClass(InvertedIndexMapper.class);
	job.setCombinerClass(InvertedIndexReducer.class);
	job.setReducerClass(InvertedIndexReducer.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	Path in = new Path(args[0]);
	Path out = new Path(args[1]);
	// 删除已存在的输出文件
	out.getFileSystem(conf).delete(out, true);
	FileInputFormat.addInputPath(job, in);
	FileOutputFormat.setOutputPath(job, out);
	int ret = job.waitForCompletion(true) ? 0 : 1;
	return ret;
    }

    public static void main(String[] args) throws Exception {
	String[] path = { "hdfs://10.21.71.132:9000/user/hadoop/input/invertedindex/",
		"hdfs://10.21.71.132:9000/user/hadoop/output/InvertedIndex" };
	int res = ToolRunner.run(new Configuration(), new InvertedIndex(), path);
	System.exit(res);
    }

}