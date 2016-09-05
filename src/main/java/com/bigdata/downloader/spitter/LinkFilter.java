package com.bigdata.downloader.spitter;

/**
 * 过滤链接
 * 
 * @author Cang
 *
 */
public interface LinkFilter {
    
    /**
     * 判断链接是否能够合法与有效
     * @param url
     * @return
     */
    public boolean accept(String url);
}