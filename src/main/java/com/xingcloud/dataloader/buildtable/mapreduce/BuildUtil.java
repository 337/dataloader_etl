package com.xingcloud.dataloader.buildtable.mapreduce;

import com.xingcloud.dataloader.lib.HdfsPath;
import com.xingcloud.util.ProjectInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Author: qiujiawei
 * Date:   12-6-18
 */
public class BuildUtil {
    public static final Log LOG = LogFactory.getLog(BuildUtil.class);
    static public List<Path> listDir(String dir) throws URISyntaxException, IOException {
        List<Path> re=new ArrayList<Path>();
        FileSystem fs= FileSystem.get(new URI(dir), new Configuration());
        if(fs.exists(new Path(dir))){
            for(FileStatus status:fs.listStatus(new Path(dir))){
                Path temp=status.getPath();
                if(fs.isFile(temp)){
                    re.add(temp);
                }
            }
        }
        return re;
    }

    static public TreeSet<String> getProjectList(){
        try{
            String dir= HdfsPath.hdfsRoot+"/user/hadoop/analytics/";
            FileSystem fs=FileSystem.get(new URI(dir),new Configuration());
            FileStatus[] status = fs.listStatus(new Path(dir));
            if(status==null) {
                LOG.error("no appid!");
                return null;
            }
            TreeSet<String> result=new TreeSet<String>();
            for(FileStatus fileStatus:status){
//                LOG.debug(fileStatus.getPath());
                if(fileStatus.isDir()){

                    String appid=fileStatus.getPath().getName();
					ProjectInfo projectInfo=ProjectInfo.getProjectInfoFromAppidOrProject(appid);
					if(projectInfo!=null){
						result.add(projectInfo.getProject());
					}
                }
            }
            return result;
        }
        catch (Exception e){
            LOG.error("get all project catch Exception",e);
        }
        return null;
    }
    static public TreeSet<String> getAppidList(){
        try{
            String dir= HdfsPath.hdfsRoot+"/user/hadoop/analytics/";
            FileSystem fs=FileSystem.get(new URI(dir),new Configuration());
            FileStatus[] status = fs.listStatus(new Path(dir));
            if(status==null) {
                LOG.error("no appid!");
                return null;
            }
            TreeSet<String> result=new TreeSet<String>();
            for(FileStatus fileStatus:status){
//                LOG.debug(fileStatus.getPath());
                if(fileStatus.isDir()){

                    String appid=fileStatus.getPath().getName();
						result.add(appid);
	            }
            }
            return result;
        }
        catch (Exception e){
            LOG.error("get all project catch Exception",e);
        }
        return null;
    }

    static public List<Path> getUserCollPath(String project)
                throws URISyntaxException, IOException {
        List<Path> re = new ArrayList<Path>();
        String pathRoot = HdfsPath.hdfsRoot + "/user/hadoop/analytics/";
        FileSystem fs = FileSystem.get(new URI(pathRoot), new Configuration());
        for (FileStatus status : fs.listStatus(new Path(pathRoot))) {
            Path temp = status.getPath();
            if (!fs.isFile(temp)) {
                String appid = temp.getName();
                ProjectInfo projectInfo = ProjectInfo
                        .getProjectInfoFromAppidOrProject(appid);
                if (projectInfo == null)
                    continue;
                if (projectInfo.getProject().equals(project)) {
                    re.addAll(listDir(pathRoot + appid + "/user/coll/"));
                }
            }
        }
        return re;
    }
}
