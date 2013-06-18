package com.xingcloud.dataloader.server;

import com.xingcloud.dataloader.dao.MessageQueue;
import com.xingcloud.dataloader.server.message.*;
import com.xingcloud.util.Log4jProperties;
import com.xingcloud.util.manager.NetManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * 监听redis的消息队列，获取消息并运行
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class DataLoaderETLWatcherCoin {


    static public Log LOG = LogFactory.getLog(DataLoaderETLWatcherCoin.class);
    private String ip;

    //任务执行线程
    private DataLoaderExecutor dataLoaderExecutor;

    //消息队列
    private MessageQueue messageQueue;

    /**
     * 监听某IP的守护进程
     *
     * @param inip redis要监听的消息队列的key
     */
    public DataLoaderETLWatcherCoin(String inip) {
        if (inip != null) ip = inip;
        else ip = NetManager.getSiteLocalIp();
        messageQueue = new MessageQueue(ip);
        dataLoaderExecutor = DataLoaderExecutor.getInstance();
    }

    /**
     * 主函数，不断获取消息
     */
    public void run() throws IOException, InterruptedException {

//        Thread monitorThread = new Thread(new ETLScheduleMonitor());
//        monitorThread.start();
//        LOG.info("monitor thread :" + ip);
        LOG.info("start watcher ip:" + ip);
        try {
            while (true) {
                boolean leave = process(messageQueue.getNextMassage());
                if (leave) break;
            }
            ETLScheduleMonitor.ifStillRunn = false;
            LOG.info("watcher exit");
        } catch (Exception e) {
            LOG.info("watcher exit with exception", e);
        }
    }

    /**
     * 处理消息，返回是否要离开程序
     *
     * @param message 要处理的消息
     * @return true 主线程关闭，false 主线程不关闭
     */

    private boolean process(DataLoaderMessage message) {
        LOG.info("message queue:" + message);
        boolean leave = false;
        try {
            if (message == null) return false;
            if (message instanceof TaskMessage) {
                TaskMessage taskMessage = (TaskMessage) message;
                dataLoaderExecutor.addTask(new DataLoaderNodeTask(taskMessage.getDate(), taskMessage.getIndex(),
                        taskMessage.getProject(), taskMessage.getRunType(), taskMessage.getTaskPriority()));
            } else if (message instanceof WaitAndExitMessage) {
                dataLoaderExecutor.shutdown();
                leave = true;
            } else if (message instanceof ExitMessage) {
                dataLoaderExecutor.shutdownNow();
                leave = true;
            } else if (message instanceof NullMessage) {
                Thread.sleep(10000);
            } else if (message instanceof PrintTaskMessage) {
                dataLoaderExecutor.printStats();
            }

        } catch (Exception e) {
            LOG.error("process event catch exception" + message, e);
        }
        return leave;
    }

    /**
     * 启动函数 传入可监听IP，不传入即为使用本地默认内网IP
     *
     * @param args
     */

    static public void main(String[] args) throws IOException, InterruptedException {
        String ip = null;
        if (args.length > 0) ip = args[0];
        Log4jProperties.init();
        DataLoaderETLWatcherCoin dataLoaderWatcher = new DataLoaderETLWatcherCoin(ip);
        dataLoaderWatcher.run();
    }
}
