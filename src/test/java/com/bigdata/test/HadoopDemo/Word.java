package com.bigdata.test.HadoopDemo;

public class Word implements Comparable<Word> {

    public String value;
    public int frequency;

    public Word(String value, int frequency) {
	this.value = value;
	this.frequency = frequency;
    }

    @Override
    public int compareTo(Word o) {
	return o.frequency - this.frequency;
    }

    @Override
    public boolean equals(Object obj) {
	if (obj instanceof Word) {
	    return value.equalsIgnoreCase(((Word) obj).value);
	} else {
	    return false;
	}
    }
}