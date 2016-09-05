package com.bigdata.test.other;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * BigDecimal能够用于精确计算
 * 
 * @author cang
 *
 */
public class FloatNumberDemo {
    public static void main(String[] args) throws IOException {
	double d1 = 0.3434;
	double d2 = 0.2343;
	BigDecimal b1 = new BigDecimal(d1);
	BigDecimal b2 = new BigDecimal(d2);
	System.out.println(b1.multiply(b2).doubleValue());
	System.out.println(d1 * d2);
    }
}