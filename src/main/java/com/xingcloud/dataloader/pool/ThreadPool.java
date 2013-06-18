package com.xingcloud.dataloader.pool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.*;

public class ThreadPool {
    private static Log logger = LogFactory.getLog(ThreadPool.class);
    private ThreadPoolExecutor executor;
    private int DEFAULT_THREAD_NUM = 10;
    private long TIMEOUT = 10*60*60;
    private boolean isShutDown = false;
    
    private static ThreadPool m_instance;
    
    private ThreadPool() {
        logger.info("First time init task pool");
        ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setNameFormat("Task pool");
        builder.setDaemon(true);
        ThreadFactory factory = builder.build();
        executor = (ThreadPoolExecutor)Executors.newFixedThreadPool(DEFAULT_THREAD_NUM, factory);
        
    }
    
    public synchronized static ThreadPool getInstance() {
        if (m_instance == null) {
            m_instance = new ThreadPool();
        }
        return m_instance;
    }
    
    public ThreadPoolExecutor getPool() {
        return executor;
    }

    public static FutureTask<?> addTask(Callable<?> task) {
        return  (FutureTask<?>)ThreadPool.getInstance().getPool().submit(task);
    }

    public static void addTask(Thread task) {
        ThreadPool.getInstance().getPool().submit(task);
    }
    
    public synchronized void shutDownNow() {
        if (!isShutDown)  {
            logger.info("------Shut down all tasks in thread pool------");
            executor.shutdown();
            /*Wait for all the tasks to finish*/
            try {
                boolean stillRunning = !executor.awaitTermination(TIMEOUT, TimeUnit.SECONDS);
                if (stillRunning) {
                    try {
                        executor.shutdownNow();
                    } catch (Exception e) {
                        logger.error("Thread pool remain query tasks' time out of time for 30 seconds.", e);
                    }
                }
            } catch (InterruptedException e) {
                try {
                    Thread.currentThread().interrupt();
                } catch (Exception e1) {
                    logger.error("Thread pool has been interrupted!", e);
                }
            }
            isShutDown = true;
        }
    }   
    
    public static synchronized void shutDownAllTasks() {
        ThreadPool.getInstance().shutDownNow();
    }
    
}
