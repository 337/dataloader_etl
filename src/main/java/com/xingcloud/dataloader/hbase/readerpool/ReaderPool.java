package com.xingcloud.dataloader.hbase.readerpool;


import com.xingcloud.dataloader.StaticConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Author: qiujiawei
 * Date:   12-6-25
 */
public class ReaderPool {
    public static final Log LOG = LogFactory.getLog(ReaderPool.class);
    public static final int ThreadPoolMaxTime=2*3600;

    private static final SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

//        protected SubmitThreadPool threadPool;

    private ThreadPoolExecutor exec;
    private int index=0;
    public ReaderPool(){
        LOG.info("readerPool thread number:"+StaticConfig.readerPoolThreadNumber);
        exec = new ThreadPoolExecutor(StaticConfig.readerPoolThreadNumber, StaticConfig.readerPoolThreadNumber,
                30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    }
    public void submit(ReaderTask readerTask){
        exec.execute(readerTask);
    }
    public boolean shutdown(){
        boolean result=true;
        try{
            exec.shutdown();
            exec.awaitTermination(ThreadPoolMaxTime,TimeUnit.SECONDS);
//            if(exec.awaitTermination(ThreadPoolMaxTime,TimeUnit.SECONDS)){
//                LOG.info("pool finish less than "+ThreadPoolMaxTime);
//            }
//            else{
//                LOG.error("pool finish more than "+ThreadPoolMaxTime+"se and kill");
//                exec.shutdownNow();
//                exec.awaitTermination(1,TimeUnit.HOURS);
//                Date date=new Date();
//                MailManager.getInstance().sendEmail("dataloader pool could not finish less than "+ThreadPoolMaxTime+"se and kill",
//                        NetManager.getPublicIp()+","+NetManager.getSiteLocalIp()+": at"+df.format(date));
//                result=false;
//            }
        }
        catch (Exception e){
            LOG.error("pool shutdown catch Exception", e);
            result=false;
        }
        return result;
    }
    public void shutdownNow(){
        try{
            List<Runnable> re = exec.shutdownNow();
            LOG.info(re.size()+" is del");
            exec.awaitTermination(1,TimeUnit.HOURS);
        }
        catch (Exception e){
            LOG.error("pool shutdown catch Exception", e);
        }
    }

}
