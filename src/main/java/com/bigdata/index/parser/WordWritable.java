package com.bigdata.index.parser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WordWritable implements WritableComparable<WordWritable> {

    private Text docUrl = new Text();
    private IntWritable tittleFreq = new IntWritable();
    private IntWritable tittleCount = new IntWritable();
    private IntWritable contentFreq = new IntWritable();
    private IntWritable contentCount = new IntWritable();

    public WordWritable() {
    }

    public WordWritable(String url, int tittleFreq, int tittleCount) {
	this.docUrl.set(url);
	this.tittleFreq.set(tittleFreq);
	this.tittleCount.set(tittleCount);
    }

    @Override
    public void write(DataOutput out) throws IOException {
	this.docUrl.write(out);
	this.tittleFreq.write(out);
	this.tittleCount.write(out);
	this.contentFreq.write(out);
	this.contentCount.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	this.docUrl.readFields(in);
	this.tittleFreq.readFields(in);
	this.tittleCount.readFields(in);
	this.contentFreq.readFields(in);
	this.contentCount.readFields(in);

    }

    public void tittleFreqIncreasement() {
	tittleFreq.set(tittleFreq.get() + 1);
    }

    public void tittleCountIncreasement() {
	tittleCount.set(tittleCount.get() + 1);
    }

    public void contentFreqIncreasement() {
	contentFreq.set(contentFreq.get() + 1);
    }

    public void contentCountIncreasement() {
	contentCount.set(contentCount.get() + 1);
    }

    public Text getDocUrl() {
	return docUrl;
    }

    public void setDocUrl(Text docUrl) {
	this.docUrl = docUrl;
    }

    public IntWritable getTittleFreq() {
	return tittleFreq;
    }

    public void setTittleFreq(int value) {
	this.tittleFreq.set(value);
    }

    public IntWritable getTittleCount() {
	return tittleCount;
    }

    public void setTittleCount(int tittleCount) {
	this.tittleCount.set(tittleCount);
    }

    public IntWritable getContentFreq() {
	return contentFreq;
    }

    public void setContentFreq(int contentFreq) {
	this.contentFreq.set(contentFreq);
    }

    public IntWritable getContentCount() {
	return contentCount;
    }

    public void setContentCount(int contentCount) {
	this.contentCount.set(contentCount);
    }

    @Override
    public int compareTo(WordWritable o) {
	return this.docUrl.compareTo(o.docUrl);
    }

    @Override
    public String toString() {
	return docUrl + "," + tittleFreq + "," + tittleCount + "," + contentFreq + ","
		+ contentCount;
    }

}
