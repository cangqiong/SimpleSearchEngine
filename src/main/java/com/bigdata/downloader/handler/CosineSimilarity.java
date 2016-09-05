package com.bigdata.downloader.handler;

import java.util.Map;
import java.util.Map.Entry;

import org.ansj.app.keyword.Keyword;

/**
 * 计算余弦相似度<br>
 * 有a向量[x1,y1],b向量[x2,y2]<br>
 * 公式：cosθ=（x1*x2+y1*y2）/sqrt(x1*x1+y1*y1)*sqrt(x2*x2 +y2*y2)
 * 
 * @author Cang
 *
 */
public class CosineSimilarity {

    /**
     * 
     * @param totalKeyWord
     * @param pageVector
     * @return
     */
    private static double sqrtMulti(Map<String, Keyword> totalKeyWord,
	    Map<String, Keyword> pageVector) {
	double result = 0;
	result = Math.sqrt(squares(totalKeyWord) * squares(pageVector));
	return result;
    }

    /**
     * 求平方和
     * 
     * @param totalKeyWord
     * @return
     */
    private static double squares(Map<String, Keyword> totalKeyWord) {
	double result = 0;
	for (Keyword word : totalKeyWord.values()) {
	    result += Math.pow(word.getScore(), 2);
	}
	return result;
    }

    /**
     * 点乘法
     * 
     * @param totalKeyWord
     * @param pageVector
     * @return
     */
    private static double pointMulti(Map<String, Keyword> totalKeyWord,
	    Map<String, Keyword> pageVector) {
	double result = 0;
	for (Entry<String, Keyword> e : totalKeyWord.entrySet()) {
	    Keyword word = pageVector.get(e.getKey());
	    if (word == null) {
		continue;
	    } else {
		result += e.getValue().getScore() * word.getScore();
	    }
	}
	return result;
    }

    /**
     * 计算余弦相似度
     * 
     * @param totalKeyWord
     * @param pageVector
     * @return
     */
    public static double count(Map<String, Keyword> totalKeyWord, Map<String, Keyword> pageVector) {
	double result = 0;
	result = pointMulti(totalKeyWord, pageVector) / sqrtMulti(totalKeyWord, pageVector);
	return result;
    }

}
