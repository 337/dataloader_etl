package com.xingcloud.dataloader.buildtable.mapreduce.builddeu;

import com.xingcloud.dataloader.BuildTableAdmin;
import com.xingcloud.dataloader.buildtable.mapreduce.BuildUtil;
import com.xingcloud.dataloader.hbase.table.DeuTable;
import com.xingcloud.dataloader.lib.HdfsPath;
import com.xingcloud.util.Log4jProperties;
import com.xingcloud.util.ProjectInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * 修复deu表 ,要求hdfs中的/user/hadoop/analytics/${appid}/${year}/${month}/ea_data_${day}/data 或者 /user/hadoop/analytics/${appid}/${year}/${month}/ea_data_${day}/store 中有对应的日志
    参数：
    @启动类 main class
    @项目控制 ${project} | all | begin:${project}
    @日期 ${year}${month}${day}
    hadoop jar /home/hadoop/xa/runJar/dataloader_mapreduce.jar com.xingcloud.dataloader.buildtable.mapreduce.builddeu.BuildDeuJob all ${date}
 * Author: qiujiawei
 * Date:   12-4-26
 */
public class BuildDeuJob {
    public static final Log LOG = LogFactory.getLog(BuildDeuJob.class);
    static public void  main(String[] args){
        Log4jProperties.init();
        BuildDeuJob buildDeuJob =new BuildDeuJob();
        String beginProject=null;
        if(args.length>1 && args[0]!=null){
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

            List<String> runList=new ArrayList<String>();

            for(String project:projects){
                if(beginProject == null || project.compareTo(beginProject)>=0){
                    runList.add(project);
                }
            }
            LOG.info("ready to run:"+runList);
            for(String project:runList){
                buildDeuJob.run(project, args[1]);
            }
            long end=System.currentTimeMillis();
            LOG.info("all job finish using"+(end-start)/1000);
        }

    }
    private String pathPrefix= HdfsPath.hdfsRoot+"/user/hadoop/builddeutable/";
    public void run(String project,String  date){
        try{
            LOG.info("run job " + project + " " + date);
            BuildTableAdmin.ensureProjectTable(project);

            Configuration conf = HBaseConfiguration.create();
//            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            conf.set("project",project);

            Job job = new Job(conf, project+" "+date);
            job.setJarByClass(BuildDeuJob.class);

            job.setMapperClass(BuildDeuMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);

            FileSystem fs=FileSystem.get(new URI(pathPrefix),new Configuration());
            boolean add=false;
            long total=0;
            for(Path path:getLogPath(project, date)){
                LOG.info(path.toString());
                add=true;
                total+=fs.getFileStatus(path).getLen();
                FileInputFormat.addInputPath(job, path);
            }

//            job.setNumReduceTasks((int) (total/(128*1024*1024)));
//
//            job.setOutputFormatClass(TableOutputFormat.class);
//            job.setOutputKeyClass(ImmutableBytesWritable.class);
//            job.setOutputValueClass(Writable.class);
//
//            DeuTable deuTable=new DeuTable();
//            TableMapReduceUtil.initTableReducerJob(deuTable.getTableName(project.toLowerCase()), BuildDeuReducer.class, job);
            FileOutputFormat.setOutputPath(job, getOutputPath(project));

            boolean re=false;
            if(add){
                re=job.waitForCompletion(true);
                if(re){
                LOG.info("yeah! job successful");
                }
                else{
                    LOG.error("CAO JOB FAILED");
                }
            }
            else{
                LOG.info("no input");
            }


        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 返回一个项目下所有的日志文件
     * @param project  项目名称
     * @param date     日期
     * @return  所有的日志文件的绝对目录
     * @throws URISyntaxException
     * @throws IOException
     */
    private List<Path> getLogPath(String project,String date) throws URISyntaxException, IOException {
        String year=date.substring(0, 4);
        String month=date.substring(4,6);
        String day=date.substring(6,8);
        List<Path> re=new ArrayList<Path>();
        String pathRoot= HdfsPath.hdfsRoot+"/user/hadoop/analytics/";
        if (project.equals("v9-KLT") || project.equals("v9-klt")) {
            re.addAll(BuildUtil.listDir(pathRoot + "v9-KLT" + "/" + year + "/" + month + "/ea_data_" + day + "/data/"));
            re.addAll(BuildUtil.listDir(pathRoot + "v9-KLT" + "/" + year + "/" + month + "/ea_data_" + day + "/store/"));
            re.addAll(BuildUtil.listDir(pathRoot + "v9-klt" + "/" + year + "/" + month + "/ea_data_" + day + "/data/"));
            re.addAll(BuildUtil.listDir(pathRoot + "v9-klt" + "/" + year + "/" + month + "/ea_data_" + day + "/store/"));
            return re;
        }


        FileSystem fs= FileSystem.get(new URI(pathRoot), new Configuration());
        for(FileStatus status:fs.listStatus(new Path(pathRoot))){
            Path temp=status.getPath();
            if(!fs.isFile(temp)){
                String appid=temp.getName();
                ProjectInfo projectInfo=ProjectInfo.getProjectInfoFromAppidOrProject(appid);
                if(projectInfo==null) continue;
                if(projectInfo.getProject().equals(project)){
                    re.addAll(BuildUtil.listDir(pathRoot + appid + "/" + year + "/" + month + "/ea_data_" + day + "/data/"));
                    re.addAll(BuildUtil.listDir(pathRoot + appid + "/" + year + "/" + month + "/ea_data_" + day + "/store/"));
                }
            }
        }
        return re;
    }

    private Path getOutputPath(String appid) throws URISyntaxException, IOException {
        FileSystem fs= FileSystem.get(new URI(pathPrefix), new Configuration());
        Path outputPath=new Path(pathPrefix+"/"+appid+"/");
        fs.delete(outputPath,true);
        return outputPath;
    }
    private static void printRe(Result re){
        System.out.print("property "+new String(re.getRow()));
        int debug=10;
        for(Map.Entry<byte[],byte[]> entry:re.getFamilyMap("user".getBytes()).entrySet()){
            String qu= Bytes.toString(entry.getKey());
            System.out.print(Bytes.toString(entry.getKey())+" ");
            debug--;
            if(debug<0) break;
        }
        System.out.println();
    }


}
