package com.bigdata.index.parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bigdata.downloader.spitter.Crawler;
import com.bigdata.util.HBaseUtils;

/**
 * 为每个文档建立索引
 * 
 * @author Cang
 *
 */
public class CreateIndex extends Configured implements Tool {
    private static Set<String> stopWords = new TreeSet<>();
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

    public static class CreateIndexMapper extends TableMapper<Text, WordWritable> {

	private Map<String, WordWritable> tokenMap = new HashMap<>();

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
		throws IOException, InterruptedException {
	    WordWritable temp = null;
	    String url = Bytes.toString(value.getRow());
	    String title = Bytes.toString(
		    value.getValue(Bytes.toBytes("contents"), Bytes.toBytes("title")))
		    .toLowerCase();
	    String content = Bytes.toString(
		    value.getValue(Bytes.toBytes("contents"), Bytes.toBytes("text"))).toLowerCase();
	    // 对全文进行分词
	    List<Term> titleTerms = ToAnalysis.parse(title);
	    int titleCount = 0;
	    for (Term term : titleTerms) {
		if (stopWords.contains(term.getName())) {
		    continue;
		}
		titleCount++;
		if (tokenMap.containsKey(term.getName())) {
		    temp = tokenMap.get(term.getName());
		    temp.tittleFreqIncreasement();
		} else {
		    tokenMap.put(term.getName(), new WordWritable(url, 1, 0));
		}
	    }
	    List<Term> contentTerms = ToAnalysis.parse(content);
	    int contentCount = 0;
	    for (Term term : contentTerms) {
		if (stopWords.contains(term.getName())) {
		    continue;
		}
		contentCount++;
		if (tokenMap.containsKey(term.getName())) {
		    temp = tokenMap.get(term.getName());
		    temp.contentFreqIncreasement();
		} else {
		    tokenMap.put(term.getName(), new WordWritable(url, 0, 1));
		}
	    }
	    for (Entry<String, WordWritable> entry : tokenMap.entrySet()) {
		WordWritable w = entry.getValue();
		w.setTittleCount(titleCount);
		w.setContentCount(contentCount);
		context.write(new Text(entry.getKey()), w);
	    }
	    Put put = new Put(key.get());
	    put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("indexed"),
		    Bytes.toBytes("true"));
	    HBaseUtils.PutData("url_info", put);
	}
    }

    public static class CreateIndexReducer extends
	    TableReducer<Text, WordWritable, ImmutableBytesWritable> {

	@Override
	protected void reduce(Text key, Iterable<WordWritable> values, Context context)
		throws IOException, InterruptedException {
	    StringBuilder positions = new StringBuilder();
	    for (WordWritable WordWritable : values) {
		positions.append(WordWritable.toString() + "#");
	    }
	    positions.substring(0, positions.lastIndexOf("#"));
	    System.out.println(positions);
	    Put put = new Put(key.getBytes());
	    put.addColumn(Bytes.toBytes("index"), Bytes.toBytes("string"),
		    Bytes.toBytes(positions.toString()));
	    context.write(new ImmutableBytesWritable(key.getBytes()), put);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = HBaseConfiguration.create();
	conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	Job job = Job.getInstance(conf, "CreateIndex");
	job.setJarByClass(CreateIndex.class);
	// job.setMapperClass(CreateIndexMapper.class);
	// job.setOutputKeyClass(Text.class);
	// job.setOutputValueClass(WordWritable.class);
	// job.setMapOutputKeyClass(Text.class);
	// job.setMapOutputValueClass(WordWritable.class);
	// Path out = new Path(args[1]);
	// out.getFileSystem(conf).delete(out, true);
	// FileOutputFormat.setOutputPath(job, out);
	job.setNumReduceTasks(1);

	Scan scan = new Scan();
	// 是否缓存块
	scan.setCacheBlocks(false);
	// 每次从服务器端读取的行数，默认为配置文件中设置的值
	scan.setCaching(10);

	Filter filter = new SingleColumnValueFilter(Bytes.toBytes("contents"),
		Bytes.toBytes("indexed"), CompareOp.EQUAL, Bytes.toBytes("false"));
	scan.setFilter(filter);
	scan.setCacheBlocks(false);
	// 指定Mapper读取的表为url_info
	TableMapReduceUtil.initTableMapperJob("url_info", scan, CreateIndexMapper.class,
		Text.class, WordWritable.class, job);
	TableMapReduceUtil.initTableReducerJob("index_info", CreateIndexReducer.class, job);
	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new CreateIndex(), args);
	System.exit(res);
    }

}