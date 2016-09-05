package com.bigdata.index.parser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class SingleRecordWritable implements Writable {

    private String docUrl;
    private int tittleFreq;
    private int tittleCount;
    private int contentFreq;
    private int contentCount;

    public SingleRecordWritable() {
    }

    public SingleRecordWritable(String docUrl, int tittleFreq, int contentFreq) {
	this.docUrl = docUrl;
	this.tittleFreq = tittleFreq;
	this.contentFreq = contentFreq;
    }

    @Override
    public void write(DataOutput out) throws IOException {
	out.writeChars(docUrl);
	out.writeInt(tittleFreq);
	out.writeInt(tittleCount);
	out.writeInt(contentFreq);
	out.writeInt(contentCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
	this.docUrl = in.readLine();
	this.tittleFreq = in.readInt();
	this.tittleCount = in.readInt();
	this.contentFreq = in.readInt();
	this.contentCount = in.readInt();

    }

    public void tittleFreqIncreasement() {
	tittleFreq++;
    }

    public void tittleCountIncreasement() {
	tittleCount++;
    }

    public void contentFreqIncreasement() {
	contentFreq++;
    }

    public void contentCountIncreasement() {
	contentCount++;
    }

    public String getDocUrl() {
	return docUrl;
    }

    public void setDocUrl(String docUrl) {
	this.docUrl = docUrl;
    }

    public int getTittleFreq() {
	return tittleFreq;
    }

    public void setTittleFreq(int tittleFreq) {
	this.tittleFreq = tittleFreq;
    }

    public int getTittleCount() {
	return tittleCount;
    }

    public void setTittleCount(int tittleCount) {
	this.tittleCount = tittleCount;
    }

    public int getContentFreq() {
	return contentFreq;
    }

    public void setContentFreq(int contentFreq) {
	this.contentFreq = contentFreq;
    }

    public int getContentCount() {
	return contentCount;
    }

    public void setContentCount(int contentCount) {
	this.contentCount = contentCount;
    }

    public int compareTo(SingleRecordWritable o) {
	return this.docUrl.compareTo(o.docUrl);
    }

    @Override
    public String toString() {
	return docUrl + "," + tittleFreq + "," + tittleCount + "," + contentFreq + ","
		+ contentCount;
    }

}
