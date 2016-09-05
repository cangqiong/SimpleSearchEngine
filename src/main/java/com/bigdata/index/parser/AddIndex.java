package com.bigdata.index.parser;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
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

import com.bigdata.util.HBaseUtils;

/**
 * 为每个文档建立索引
 * 
 * @author Cang
 *
 */
public class AddIndex extends Configured implements Tool {

    public static class CreateIndexMapper extends TableMapper<Text, Text> {

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
		throws IOException, InterruptedException {
	    Put put = new Put(key.get());
	    System.out.println(Bytes.toString(key.get()));
	    put.addColumn(Bytes.toBytes("contents"), Bytes.toBytes("indexed"),
		    Bytes.toBytes("false"));
	    HBaseUtils.PutData("url_info", put);
	}
    }

    @Override
    public int run(String[] args) throws Exception {
	Configuration conf = HBaseConfiguration.create();
	conf.setBoolean("mapred.map.tasks.speculative.execution", false);
	Job job = Job.getInstance(conf, "CreateIndex");
	job.setJarByClass(AddIndex.class);
	job.setNumReduceTasks(0);

	Scan scan = new Scan();
	// 是否缓存块
	scan.setCacheBlocks(false);
	// 每次从服务器端读取的行数，默认为配置文件中设置的值
	scan.setCaching(10);

	TableMapReduceUtil.initTableMapperJob("url_info", scan, CreateIndexMapper.class,
		Text.class, IntWritable.class, job);
	Path out = new Path("hdfs://10.21.71.132:9000/user/hadoop/output/Inver4tedfdfgIndex");
	FileOutputFormat.setOutputPath(job, out);
	return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new AddIndex(), args);
	System.exit(res);
    }

}