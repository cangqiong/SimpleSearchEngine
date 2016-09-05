package com.bigdata.downloader;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * HttpClient连接池
 * @author Cang
 *
 */
public class PoolManager {
    public static PoolingHttpClientConnectionManager clientConnectionManager = null;

    private int maxTotal = 50;

    private int defaultMaxPerRoute = 25;

    private PoolManager(int maxTotal, int defaultMaxPerRoute){
        this.maxTotal = maxTotal;
        this.defaultMaxPerRoute = defaultMaxPerRoute;
        clientConnectionManager.setMaxTotal(maxTotal);
        clientConnectionManager.setDefaultMaxPerRoute(defaultMaxPerRoute);
    }
    private PoolManager(){
        clientConnectionManager.setMaxTotal(maxTotal);
        clientConnectionManager.setDefaultMaxPerRoute(defaultMaxPerRoute);
    }

    private static PoolManager poolManager = null;

    public synchronized static PoolManager getInstance(){
        if(poolManager == null){
            clientConnectionManager = new PoolingHttpClientConnectionManager();
            poolManager = new PoolManager();
        }

        return poolManager;
    }
    
    public synchronized static PoolManager getInstance(int maxTotal, int defaultMaxPerRoute){
        if(poolManager == null){
            poolManager = new PoolManager(maxTotal, defaultMaxPerRoute);
        }

        return poolManager;
    }

    public static CloseableHttpClient getHttpClient(){
        if(clientConnectionManager == null){
            clientConnectionManager = new PoolingHttpClientConnectionManager();
            getInstance();
        }

        return HttpClients.custom().setConnectionManager(clientConnectionManager).build();
    }
}