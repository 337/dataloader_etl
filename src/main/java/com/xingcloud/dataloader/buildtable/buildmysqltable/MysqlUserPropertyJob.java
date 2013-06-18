package com.xingcloud.dataloader.buildtable.buildmysqltable;

import com.xingcloud.dataloader.buildtable.mapreduce.BuildUtil;
import com.xingcloud.dataloader.lib.HdfsPath;
import com.xingcloud.util.Log4jProperties;
import com.xingcloud.util.ProjectInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Author: qiujiawei Date: 12-6-7
 */
public class MysqlUserPropertyJob {
    public static final Log LOG = LogFactory.getLog(MysqlUserPropertyJob.class);

    static public void main(String[] args) {
        Log4jProperties.init();
        MysqlUserPropertyJob buildUserInfoJob = new MysqlUserPropertyJob();
        String beginProject=null;
        if(args.length>0 && args[0]!=null){
            long start=System.currentTimeMillis();
            Set<String> projects;
            if(args[0].equals("all") || args[0].startsWith("begin:")){
                projects= BuildUtil.getProjectList();
            }
            else {
                projects=new HashSet<String>();
                projects.add(args[0]);
            }

            if(args[0].startsWith("begin")){
                beginProject=args[0].substring(6);
            }

            List<String> sortList=new ArrayList<String>();
            List<String> runList=new ArrayList<String>();
            for(String project:projects)sortList.add(project);
            Collections.sort(sortList);

            for(String project:sortList){
                if(beginProject == null || project.compareTo(beginProject)>=0){
                    runList.add(project);
                }
            }
            LOG.info("ready to run:"+runList);
            for(String project:runList){
                buildUserInfoJob.run(project);
            }
            long end=System.currentTimeMillis();
            LOG.info("all job finish using"+(end-start)/1000);
        }
    }

    static String[] getAllProject(String allProject) {
        return allProject.split(",");
    }

    private String pathPrefix = HdfsPath.hdfsRoot
            + "/user/hadoop/buildmysqltable/";

    public void run(String project) {
        try {
            Path output = getOutputPath(project);
            Configuration conf = new Configuration();

            Job job = new Job(conf, "user:"+project);
            job.setJarByClass(MysqlUserPropertyJob.class);

            job.setMapperClass(UserPropertyMapper.class);
            job.setReducerClass(UserPropertyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
            // job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputFormatClass(HotColdMultipleTextOutputFormat.class);

            boolean add = false;
            for (Path path : getUserCollPath(project)) {
                LOG.info(path.toString());
                add = true;
                FileInputFormat.addInputPath(job, path);
            }
            FileOutputFormat.setOutputPath(job, output);

            if (add) {
                job.waitForCompletion(true);
                System.out.println("build finish");
            }

            String LocalOutputDir = "/data/mysql/dataloader/" + project;

            File localFile = new File(LocalOutputDir);
            if (!localFile.exists()) {
                localFile.mkdirs();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<Path> getUserCollPath(String project)
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

    private List<Path> listDir(String dir) throws URISyntaxException,
            IOException {
        List<Path> re = new ArrayList<Path>();
        FileSystem fs = FileSystem.get(new URI(dir), new Configuration());
        if (fs.exists(new Path(dir))) {
            for (FileStatus status : fs.listStatus(new Path(dir))) {
                Path temp = status.getPath();
                if (fs.isFile(temp)) {
                    re.add(temp);
                }
            }
        }
        return re;
    }

    private Path getOutputPath(String appid) throws URISyntaxException,
            IOException {
        FileSystem fs = FileSystem
                .get(new URI(pathPrefix), new Configuration());
        Path outputPath = new Path(pathPrefix + "/" + appid + "/");
        fs.delete(outputPath, true);
        return outputPath;
    }

    /**
     * @author Z J Wu@2012-06-19
     * 
     * @param projectId
     * @param localPathList
     */
    private void loadHotDataFromFile(String projectId,
            List<String> localPathList) {

    }

    /**
     * @author Z J Wu@2012-06-19
     * 
     * @param projectId
     * @param localPathList
     */
    private void loadColdDataFromFile(String projectId,
            List<String> localPathList) {

    }


}
