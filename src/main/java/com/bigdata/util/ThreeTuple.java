package com.bigdata.util;

/**
 * 三元组，可以传递两个参数
 * 
 * @author Cang
 *
 */
public class ThreeTuple<A, B, C> extends TwoTuple<A, B> {
    public C third;

    public ThreeTuple() {
    }

    public ThreeTuple(A a, B b, C c) {
	super(a, b);
	third = c;
    }

    public C getThird() {
	return third;
    }

    public void setThird(C third) {
	this.third = third;
    }

    public String toString() {
	return "(" + first + ", " + second + ", " + third + ")";
    }
}
