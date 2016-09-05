package com.bigdata.util;

import java.util.HashMap;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

/**
 * Json工具类
 * 
 * @author cang
 *
 */
public class JsonUtils {

    public static void main(String[] args) {
	Map<String, String> map = new HashMap<String, String>();
	map.put("code", "0");
	map.put("message", "ok");
	String json = JSON.toJSONString(map);
	System.out.println(json);
	Map<String, String> d = JSON.parseObject(json, Map.class); // 反序列化
	System.out.println(d);
    }

}