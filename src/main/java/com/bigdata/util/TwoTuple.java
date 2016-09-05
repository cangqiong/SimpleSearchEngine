package com.bigdata.util;

/**
 * 两元组，可以传递两个参数
 * 
 * @author Cang
 *
 */
public class TwoTuple<A, B> {
    public A first;
    public B second;

    public TwoTuple() {
    }

    public TwoTuple(A a, B b) {
	first = a;
	second = b;
    }

    public A getFirst() {
	return first;
    }

    public void setFirst(A first) {
	this.first = first;
    }

    public B getSecond() {
	return second;
    }

    public void setSecond(B second) {
	this.second = second;
    }

    public String toString() {
	return "(" + first + ", " + second + ")";
    }
}
