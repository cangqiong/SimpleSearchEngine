package com.bigdata.test.other;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

import org.ansj.app.keyword.Keyword;

import com.bigdata.util.FileUtils;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

class JSON {

    public static void main(String args[]) {
	String test = FileUtils.File2String("key.txt");
	System.out.println(test);
	Gson gson = new Gson();
	List<Keyword> rs = new ArrayList<Keyword>();
	Type type = new TypeToken<ArrayList<Keyword>>() {
	}.getType();
	rs = gson.fromJson(test, type);
	System.out.println(rs);
    }
}