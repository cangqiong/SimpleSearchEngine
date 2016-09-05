package com.bigdata.test.other;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.bigdata.util.StringTools;

class TT

{

    public static void main(String args[])

    {
	DateFormat formatter = new SimpleDateFormat("yyyyMMddhhmmss");
	double initWeghit = 1;
	int t = (int) (100000 * (1 - initWeghit));
	// 0 代表前面补充0
	// 4 代表长度为4
	// d 代表参数为正数型
	System.out.println(t);
	String str = String.format("%05d", t);
	System.out.println(str);
	String weghit = str + formatter.format(new Date());
	System.out.println(weghit);
	System.out.println(StringTools.getPriority(weghit));
    }
}