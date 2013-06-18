package com.xingcloud.dataloader.tools;

import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.dataloader.lib.HdfsPath;
import com.xingcloud.dataloader.lib.LocalPath;
import com.xingcloud.util.Log4jProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 将本地的bz2压缩文件转存到hdfs的history目录上（迁移数据用）
 * Author: qiujiawei
 * Date:   12-7-10
 */
public class CopyBz2ToHistory {
    public static final Log LOG = LogFactory.getLog(CopyBz2ToHistory.class);
    public static void main(String[] args){
        CopyBz2ToHistory copyBz2ToHistory=new CopyBz2ToHistory();
        copyBz2ToHistory.run(args);

    }
    private void run(String[] args){
        try{
            Log4jProperties.init();
            long t1=System.currentTimeMillis();
            File file=new File(args[0]);
            List<String> paths=new ArrayList<String>();
            if(file.isDirectory()){
                for(File f:file.listFiles()){
                    if(!f.isDirectory())  {
                        paths.add(f.getPath());
                    }
                }
            }
            else{
                paths.add(file.getPath());
            }

            ExecutorService executorService = Executors.newFixedThreadPool(StaticConfig.copyBz2ToHistoryDefaultThreadNumber);

            for(String path:paths){
                executorService.submit(new CopyTask(path));
            }
            executorService.shutdown();
            executorService.awaitTermination(1, TimeUnit.HOURS);
            long t2=System.currentTimeMillis();
            LOG.info("all finish using:"+(t2-t1)+" ms");

        }
        catch (Exception e){
            LOG.error(e);
        }
    }

    class CopyTask implements Runnable{
        private String localFilePath;
        public CopyTask(String localFilePath) {
            this.localFilePath = localFilePath;
        }

        public void run() {
            dealFile(localFilePath);
        }
        private boolean dealFile(String localFilePath){
            try{
                File file=new File(localFilePath);
                Path path=new Path(localFilePath);
                String fileName=path.getName();
    //               String fileName="age@337_en_andriod.s1.20120601.store.bz2";
                String type;
                String appid;
                String date;
                if(fileName.endsWith("data.bz2")){
                    type= LocalPath.SITE_DATA;
                    appid=fileName.substring(0, fileName.length() - 18);
                    date=fileName.substring(fileName.length()-17,fileName.length()-9);
                }
                else if(fileName.endsWith("store.bz2")) {
                    type= LocalPath.STORE_LOG;
                    appid=fileName.substring(0, fileName.length() - 19);
                    date=fileName.substring(fileName.length()-18,fileName.length()-10);
                }
                else return false;
                //age@337_en_andriod.s1.20120601.data.bz2

                String newurl=getGoalPath(appid,date,type);
                Path newPath=new Path(newurl);

                FileSystem fs=FileSystem.get(new URI(newurl),new Configuration());

                fs.copyFromLocalFile(path,newPath);

                LOG.info("finish copy from "+localFilePath+" to " +newPath);
                return true;
            }
            catch (Exception e){
                LOG.error(e);
            }
            return false;
        }
        private String getGoalPath(String appid, String date, String type){
            String year=date.substring(0, 4);
            String month=date.substring(4,6);
            String day=date.substring(6,8);
            String typePath=null;
            if(type.equals(LocalPath.SITE_DATA)){
                typePath="data";
            }
            else {
                typePath="store";
            }
            return HdfsPath.hdfsRoot+"/user/hadoop/history/"+appid+"/"+year+"/"+month+"/ea_data_"+day+"/"+typePath+".bz2";
        }
    }

}
