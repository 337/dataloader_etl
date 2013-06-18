package com.xingcloud.dataloader.hbase.pool;


import java.util.Map;
import java.util.concurrent.*;

/**
 * Author: qiujiawei
 * Date:   12-5-17
 */
public class DataLoaderThreadPoolExecutor extends ThreadPoolExecutor{
    private ConcurrentHashMap<String,Integer> activeRunnableMap=new ConcurrentHashMap<String,Integer>();
    public DataLoaderThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    public DataLoaderThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    public DataLoaderThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    public DataLoaderThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }
    @Override
    protected void beforeExecute(Thread t,Runnable r){
        activeRunnableMap.put(r.toString(),1);
        super.beforeExecute(t, r);
    }
    @Override
    protected void afterExecute(Runnable r,Throwable t){
        super.afterExecute(r,t);
        activeRunnableMap.remove(r.toString());
    }
    public Map<String,Integer> getActiveRunnable(){
        return activeRunnableMap;
    }

}
