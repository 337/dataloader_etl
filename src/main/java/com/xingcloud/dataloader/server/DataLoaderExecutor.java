package com.xingcloud.dataloader.server;


import com.xingcloud.util.thread.PrefixedThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 核心任务线程，处理所有的5分钟任务
 * <p/>
 * 单线程运行的任务队列。
 * 记录当前运行的任务，并暴露getRunningTask的接口查询当前正在运行的任务。作用是避免重复提交相同的任务。
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class DataLoaderExecutor extends ThreadPoolExecutor {
    static public Log LOG = LogFactory.getLog(DataLoaderExecutor.class);

    private String runningTask;

    private static DataLoaderExecutor instance;

    public static DataLoaderExecutor getInstance() {
        if (instance == null)
            instance = new DataLoaderExecutor();
        return instance;
    }

    private DataLoaderExecutor() {
        super(1, 1, 1, TimeUnit.DAYS, new PriorityBlockingQueue<Runnable>(), new PrefixedThreadFactory("DataLoaderExecutor"));
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        runningTask = r.toString();
        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        runningTask = "";
    }

    @Override
    public synchronized void execute(Runnable command) {
        if (command.toString().equals(getRunningTask()) || getQueue().contains(command))
            return;
        LOG.info("add" + command.toString());
        super.execute(command);
        if (getQueue().size() % 5 == 0 && getQueue().size() > 15) {
            LOG.error("dataloader_etl there is more than " + getQueue().size()
                    + " task in queue  if it is not fix task should look ");
        }

    }

    @Override
    public void shutdown() {
        getQueue().clear();
        super.shutdown();
    }

    public String getRunningTask() {
        return runningTask;
    }

    public void addTask(DataLoaderNodeTask dataLoaderNodeTask) {
        execute(dataLoaderNodeTask);
    }

    public void printStats() {
        LOG.info("task is running number:" + getActiveCount() + " task is " + getRunningTask() + " leave :" + getQueue());
    }
}
