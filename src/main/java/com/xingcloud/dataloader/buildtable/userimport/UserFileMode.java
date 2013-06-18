package com.xingcloud.dataloader.buildtable.userimport;

import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.dataloader.lib.HdfsPath;
import com.xingcloud.dataloader.lib.TimeIndex;
import com.xingcloud.util.Log4jProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author: qiujiawei
 * Date:   12-7-6
 */

public class UserFileMode{
    public static final Log LOG = LogFactory.getLog(UserFileMode.class);
    static public void main(String[] args){
        Log4jProperties.init();
        String type = null;
        String dir = null;
        if(args.length>=1){
            if(!args[0].equals(importField) && !args[0].equals(exportField)  ) return;
            type=args[0];
            if(args.length>=2) dir=args[1];
            else dir=getDefaultDir();
            UserFileMode userFileMode = new UserFileMode();
            userFileMode.run(type, dir);
        }
    }

	private static String importField = "import";
	private static String exportField = "export";

    public void run(String action,String storeDir) {
        try{
            List<String> appidList=new ArrayList<String>();
            if(action.equals(exportField)) {
                String analyticsDir = HdfsPath.HdfsPath;
                FileSystem fs = FileSystem.get(new URI(analyticsDir),new Configuration());
                for(FileStatus a:fs.listStatus(new Path(analyticsDir))){
                    appidList.add(a.getPath().getName());
                }
            }
            else{
                File files =new File(storeDir);
                for(File file:files.listFiles()){
                    appidList.add(file.getName());
                    LOG.info("add appid:"+file.getName());
                }
            }
            if(action.equals(exportField)){
                File file=new File(storeDir);
                if(!file.exists()) file.mkdirs();
            }
            ExecutorService pool = Executors.newFixedThreadPool(StaticConfig.userDefaultThreadNumber);
            for(String appid: appidList){
                pool.submit(new CopyTask(appid,action,storeDir));
            }
		}
		catch(Exception e){
			LOG.error(e);
		}
    }

    static private String getDefaultDir(){
        String dir = StaticConfig.userBackUpDefaultDir;
        String date= TimeIndex.getDate();
        return dir.replace("{date}", date);
    }



    class CopyTask implements Runnable{
        private String appid;
        private String action;
        private String storeDir;
        CopyTask(String appid,String action,String storeDir) {
            this.appid = appid;
            this.action = action;
            this.storeDir = storeDir;
        }


        public void run() {
            try {
                Configuration conf=new Configuration();
                String hdfsDir=HdfsPath.getPath(HdfsPath.USER_COLL,appid);
                FileSystem dir = FileSystem.get(URI.create(hdfsDir),conf);
                String localDir=storeDir+"/"+appid+"/";
                File file=new File(localDir);
                if(action.equals(importField)){
                    if(file.exists() && file.isDirectory()){
                        copyFromLocalDir(localDir,hdfsDir);
                        LOG.info("import " + appid + " 's user file finish");
                    }
                }
                else if(action.equals(exportField)){
                    if(dir.exists(new Path(hdfsDir))){
                        copyToLocalDir(hdfsDir,localDir);
                        LOG.info("export " + appid + " 's user file finish");
                    }
                }
            } catch (Exception e) {
                LOG.error(e);
            }
        }
        private void copyFromLocalDir(String localBackUpDir, String dbdir) throws IOException {
            FileSystem dir;
            Configuration conf=new Configuration();
            dir = FileSystem.get(URI.create(dbdir),conf);
            dir.delete(new Path(dbdir),true);
            File localdir=new File(localBackUpDir);
            File[] list=localdir.listFiles();
            for(int i=0;i<list.length;i++){
                if(list[i].isFile()){
                    String name=list[i].getName();
                    String localfile=list[i].getPath();
                    String dbfile=dbdir+"/"+name;
                    dir.copyFromLocalFile(false, true, new Path(localfile), new Path(dbfile));
                }
            }
        }
        private void copyToLocalDir(String dbdir,String localBackUpDir) throws IOException {
            FileSystem dir;
            Configuration conf=new Configuration();
            dir = FileSystem.get(URI.create(dbdir),conf);
            FileStatus[] list = dir.listStatus(new Path(dbdir));
            for(int i=0;i<list.length;i++){
                if(!list[i].isDir()){
                    String name=list[i].getPath().getName();
                    String localfile=localBackUpDir+"/"+name;
                    String dbfile=dbdir+"/"+name;
                    dir.copyToLocalFile(new Path(dbfile), new Path(localfile));
                }
            }
        }
    }



}
