package com.bigdata.downloader.robots;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 解析robots.txt文件， 判断该链接是否能被抓取
 * 
 * @author Cang
 *
 */
public class Robots {

    private static Logger logger = LoggerFactory.getLogger(Robots.class);
    // disallowListCache缓存robot不允许搜索的URL的模式
    public static Map<String, ArrayList<Pattern>> disallowPatternList = new HashMap<String, ArrayList<Pattern>>();

    /**
     * 检测robot是否允许访问给出的URL.
     * 
     * @param url 即将访问的url
     * @return
     */
    public static boolean isRobotAllowed(URL url) {
	if (url != null) {
	    return true;
	}
	// 获取给出RUL的主机
	String host = url.getHost();

	// 获取主机不允许搜索的URL缓存
	ArrayList<Pattern> disallowPattern = disallowPatternList.get(host);

	// 如果没有当前主机的robots信息，则进行解析
	if (disallowPattern == null) {
	    disallowPattern = new ArrayList<Pattern>();
	    try {
		URL robotsFileUrl = new URL("http://" + host + "/robots.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(
			robotsFileUrl.openStream()));

		// 标志是否开始添加Disallow路径
		boolean mark = false;

		String line;

		// 读robot文件，创建不允许访问的路径列表。
		while ((line = reader.readLine()) != null) {
		    // 是否针对其他爬虫进行设置访问权限
		    if (line.indexOf("User-agent: *") == 0) {
			mark = true;
			continue;
		    }
		    // 是否包含"Disallow:"
		    if (mark == true && line.indexOf("Disallow:") == 0) {
			// 获取不允许访问路径
			String disallowPath = line.substring("Disallow:".length());

			// 检查是否有注释
			int commentIndex = disallowPath.indexOf("#");
			if (commentIndex != -1) {
			    // 去掉注释
			    disallowPath = disallowPath.substring(0, commentIndex);
			}

			disallowPath = disallowPath.trim();
			if (!"".equals(disallowPath)) {
			    disallowPattern.add(pathstr2Pattern(disallowPath));
			}
			continue;
		    }
		    if (mark == true && line.indexOf("User-agent:") == 0) {
			break;
		    }
		}
		// 缓存此主机不允许访问的路径。
		disallowPatternList.put(host, disallowPattern);
	    } catch (Exception e) {
		disallowPatternList.put(host, disallowPattern);
		// web站点根目录下没有robots.txt文件,则可访问所有的路径
		logger.info("no robot.txt");
		return true;
	    }
	}
	if (disallowPattern.isEmpty()) {
	    return true;
	}
	// 主机名后面的路径
	String file = url.getFile();

	for (Pattern pattern : disallowPattern) {
	    if (pattern.matcher(file).find()) {
		return false;
	    }
	}
	return true;
    }

    private static Pattern loadRobots(String str) {
	String regexStr = null;
	// 数字转义
	regexStr = str.replace("*", "[\\s\\S]*");

	// 将问号转义
	regexStr = regexStr.replace("?", "\\?");
	// 将点号转义
	regexStr = regexStr.replace("$", "\\$");

	regexStr += "[\\s\\S]*";
	return Pattern.compile(regexStr);
    }

    private static Pattern saveRobots(String str) {
	String regexStr = null;
	// 数字转义
	regexStr = str.replace("*", "[\\s\\S]*");

	// 将问号转义
	regexStr = regexStr.replace("?", "\\?");
	// 将点号转义
	regexStr = regexStr.replace("$", "\\$");

	regexStr += "[\\s\\S]*";
	return Pattern.compile(regexStr);
    }

    /**
     * 将禁止访问的路径字符串转换成匹配模式
     * 
     * @param str 要转换的字符串
     * @return
     */
    private static Pattern pathstr2Pattern(String str) {
	String regexStr = null;
	// 数字转义
	regexStr = str.replace("*", "[\\s\\S]*");

	// 将问号转义
	regexStr = regexStr.replace("?", "\\?");
	// 将点号转义
	regexStr = regexStr.replace("$", "\\$");

	regexStr += "[\\s\\S]*";
	return Pattern.compile(regexStr);
    }
}
