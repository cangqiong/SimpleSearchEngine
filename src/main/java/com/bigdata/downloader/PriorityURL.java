package com.bigdata.downloader;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * 拥有优先级的url类
 * 
 * @author Cang
 *
 */

@Setter
@Getter
@AllArgsConstructor
public class PriorityURL implements Comparable<PriorityURL> {
    private String url;
    private double priority;

    @Override
    public int compareTo(PriorityURL o) {
	// 降序排序
	if (priority > o.priority) {
	    return -1;
	} else if (this.priority < o.priority) {
	    return 1;
	}
	return 0;
    }

}
