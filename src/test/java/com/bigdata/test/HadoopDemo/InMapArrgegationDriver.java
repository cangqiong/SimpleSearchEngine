package com.bigdata.test.HadoopDemo;


import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMapArrgegationDriver extends Configured implements Tool {
    public static Logger log = LoggerFactory.getLogger(InMapArrgegationDriver.class);

    protected static class InMapMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private ArrayList<Word> words = new ArrayList<Word>();
	private PriorityQueue<Word> queue;
	private int maxResult;

	protected void setup(Context cxt) {
	    maxResult = cxt.getConfiguration().getInt("maxResult", 10);
	}

	protected void map(LongWritable key, Text value, Context cxt) {
	    String[] line = value.toString().split(" "); // use blank to split
	    for (String word : line) {
		Word curr = new Word(word, 1);
		if (words.contains(curr)) {
		    // increase the exists word's frequency
		    for (Word w : words) {
			if (w.equals(curr)) {
			    w.frequency++;
			    break;
			}
		    }
		} else {
		    words.add(curr);
		}
	    }
	}

	protected void cleanup(Context cxt) throws InterruptedException, IOException {
	    Text outputKey = new Text();
	    IntWritable outputValue = new IntWritable();

	    queue = new PriorityQueue<Word>(words.size());
	    queue.addAll(words);
	    for (int i = 0; i < maxResult; i++) {
		Word tail = queue.poll();
		if (tail != null) {
		    outputKey.set(tail.value);
		    outputValue.set(tail.frequency);
		    log.info("key is {},value is {}", outputKey, outputValue);
		    cxt.write(outputKey, outputValue);

		}
	    }
	}
    }

    @Override
    public int run(String[] arg0) throws Exception {
	if (arg0.length != 3) {
	    System.err
		    .println("Usage:\nfz.inmap.aggregation.InMapArrgegationDriver <in> <out> <maxNum>");
	    return -1;
	}
	Configuration conf = getConf();

	// System.out.println(conf.get("fs.defaultFS"));
	Path in = new Path(arg0[0]);
	Path out = new Path(arg0[1]);
	out.getFileSystem(conf).delete(out, true);
	conf.set("maxResult", arg0[2]);
	Job job = Job.getInstance(conf, "in map arrgegation job");
	// job.setJarByClass(getClass());
	job.setJarByClass(InMapArrgegationDriver.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	job.setMapperClass(InMapMapper.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
	job.setNumReduceTasks(0);
	FileInputFormat.setInputPaths(job, in);
	FileOutputFormat.setOutputPath(job, out);

	return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
	String[] path = { "hdfs://10.21.71.132:9000/user/hadoop/input/log/",
		"hdfs://10.21.71.132:9000/user/hadoop/output/tem", "20" };
	ToolRunner.run(new Configuration(), new InMapArrgegationDriver(), path);
    }
}
