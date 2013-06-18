package com.xingcloud.dataloader.hbase.pool;

import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.util.thread.PrefixedThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Author: qiujiawei
 * Date:   12-3-20
 */
public class SubmitThreadPool {

        public static final Log LOG = LogFactory.getLog(SubmitThreadPool.class);
        public static final int ThreadPoolMaxTime=Integer.MAX_VALUE;

        private static final SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

//        protected SubmitThreadPool threadPool;

        private DataLoaderThreadPoolExecutor exec;
        private int index=0;
        public SubmitThreadPool(){
            LOG.info("submit Pool thread number:"+StaticConfig.submitPoolThreadNumber);
            exec = new DataLoaderThreadPoolExecutor(StaticConfig.submitPoolThreadNumber, StaticConfig.submitPoolThreadNumber,
                    30, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
					new PrefixedThreadFactory("SubmitThreadPool"));
        }
        public void submit(BaseTask task){
            task.setTaskNumber(index);
            exec.execute(task);
            index++;
        }
        public boolean shutdown(){
            boolean result=true;
            try{
                exec.shutdown();
                exec.awaitTermination(ThreadPoolMaxTime,TimeUnit.SECONDS);
//                if(exec.awaitTermination(ThreadPoolMaxTime,TimeUnit.SECONDS)){
//                    LOG.info("pool finish less than "+ThreadPoolMaxTime);
//                }
//                else{
//                    LOG.error("pool finish more than "+ThreadPoolMaxTime+"se and kill");
//                    exec.shutdownNow();
//                    exec.awaitTermination(1,TimeUnit.HOURS);
//                    Date date=new Date();
//                    MailManager.getInstance().sendEmail("dataloader pool could not finish less than "+ThreadPoolMaxTime+"se and kill",
//                            NetManager.getPublicIp()+","+NetManager.getSiteLocalIp()+": at"+df.format(date));
//                    result=false;
//                }
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
        public void printActiveList(){
            LOG.info("active:"+exec.getActiveCount()+"   task count:"+exec.getTaskCount()+"   complete task count:"+exec.getCompletedTaskCount());
            LOG.info("runnable list:" + exec.getActiveRunnable().size());
            LOG.info("message:"+exec.getActiveRunnable());
        }



//    /**
//     *
//     */
//    static private SubmitThreadPool instance;
//    static public SubmitThreadPool getInstance(){
//        if(SubmitThreadPool.instance==null){
//            SubmitThreadPool.instance=new SubmitThreadPool();
//        }
//        return instance;
//    }


}
