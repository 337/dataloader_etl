package com.xingcloud.dataloader.server;

import com.xingcloud.dataloader.lib.OrigIdSeqUidCacheMap;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 6/4/13
 * Time: 3:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class LoadCoinInitialStatus {
     public static Log LOG= LogFactory.getLog(LoadCoinInitialStatus.class);
     private static void process(String project,String inputPath,String outputPath){

         int batchSize=1000;
         Map<String,BufferedWriter> nodeWriter=new HashMap<String, BufferedWriter>();
         Map<String,List<String>> nodeLogs=new HashMap<String, List<String>>();
         try {
             OrigIdSeqUidCacheMap.getInstance().initCache(project);
         } catch (IOException e) {
             e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
         try {
             BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(new FileInputStream(inputPath), "UTF-8"));
             String line = null;
             for(String node: UidMappingUtil.getInstance().nodes()){
                 String path=outputPath+"_"+node;
                 BufferedWriter bufferedWriter=new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path),"UTF-8"));
                 nodeWriter.put(node,bufferedWriter);
                 List<String> logList=new ArrayList<String>();
             }
             //List<String> results=new ArrayList<String>();
             long logNum=0;
             while ((line = bufferedReader.readLine()) != null) {
                 if (line.length() < 1) continue;
                 String[] t=line.split("\t");
                 if(t.length!=2){
                     LOG.info("wrong line: "+line);
                     continue;
                 }
                 String uid=t[0];
                 String val=t[1];
                 logNum++;
                 if(logNum%10000==0)LOG.info("lognum is "+logNum);
                 long seqUid= 0;
                 try {
                     seqUid = OrigIdSeqUidCacheMap.getInstance().getUidCache(project, uid);
                 } catch (Exception e) {
                     e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                 }
                 //LOG.info("uid "+uid+" seqUid "+seqUid);
                 String node=UidMappingUtil.getInstance().hash(seqUid);
                 //LOG.info("node "+node);
                 long samplingSeqUid= UidMappingUtil.getInstance().decorateWithMD5(seqUid);
                 //LOG.info("ssuid "+samplingSeqUid);
                 String result=samplingSeqUid+"\t"+val;
                 List<String> results=nodeLogs.get(node);
                 if(results==null){
                     results=new ArrayList<String>();
                     nodeLogs.put(node,results);
                 }
                 results.add(result);
                 /*
                 if(results.size()>=batchSize) {
                     List<String> logs=results;
                     exec.submit(new WriteTask(nodeWriter.get(node),logs));
                     results=new ArrayList<String>();
                     nodeLogs.put(node,results);
                 } */
             }
             LOG.info(project+" initial state num "+logNum);
             ExecutorService exec= Executors.newFixedThreadPool(1);
             for(Map.Entry<String,List<String> >entry: nodeLogs.entrySet()){
                 if(entry.getValue().size()>0){
                     exec.submit(new WriteTask(nodeWriter.get(entry.getKey()),entry.getValue()));
                 }
             }
             exec.shutdown();
             try {
                 exec.awaitTermination(3600, TimeUnit.SECONDS);
             } catch (InterruptedException e) {
                 e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
             }
         } catch (IOException e) {
             e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
         /*

         for(String node: UidMappingUtil.getInstance().nodes()){
             String filePath=outputPath+"_"+node;
             File file=new File(filePath);
             if(file.exists()){
                 StringBuilder builder=new StringBuilder();
                 builder.append("use fix_");
                 builder.append(project);
                 builder.append(";");
                 String onceCmd = String.format("LOAD DATA LOCAL INFILE '%s' IGNORE INTO TABLE coin_initialstatus;", filePath);
                 builder.append(onceCmd);
                 String cmd = String.format("mysql -h%s -u%s -p%s -e\"%s\"", node, "xingyun", "Ohth3cha", builder.toString());
                 String[] cmds = new String[]{"/bin/sh", "-c", cmd};
                 Runtime rt = Runtime.getRuntime();
                 try {
                     LOG.info(cmd);
                     //LoadCoinInitialStatus.execShellCmd(rt, cmds);
                 } catch (Exception e) {
                     e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                 }
             }
         }
         */
     }
     public static void main(String []args){
         String projects=args[0];
         String []projectArr=projects.split("\\|");
         String inputPath="/data/log/yangbotest/coin_initial_status/origin/";
         String outputPath="/data/log/yangbotest/coin_initial_status/mysql/";
         for (int i = 0; i < projectArr.length; i++) {
             String iPath=inputPath+projectArr[i];
             String oPath=outputPath+projectArr[i];
             LoadCoinInitialStatus.process(projectArr[i],iPath,oPath);
         }
     }

    private static void execShellCmd(Runtime rt, String[] cmds) throws Exception {
        Process process = rt.exec(cmds);
        BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
        BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
        String cmdOutput = null;
        while ((cmdOutput = stdInput.readLine()) != null)
            LOG.warn(cmdOutput);
        while ((cmdOutput = stdError.readLine()) != null) {
            cmdOutput = cmdOutput.replaceAll("ERROR", "e");
            LOG.warn(cmdOutput);
        }
        int result = process.waitFor();
        if (result != 0)
            throw new RuntimeException("exec result not 0.");
    }

    private static class WriteTask implements Runnable {
        private BufferedWriter writer;
        private List<String>   logs;
        private int batchSize=10000;
        public WriteTask(BufferedWriter bufferedWriter, List<String> results) {
            writer=bufferedWriter;
            logs=results;
        }
        public void run(){
            try {
                for (int i = 0; i < logs.size(); i++) {
                    writer.write(logs.get(i));
                    writer.newLine();
                    if(i>=batchSize&&(i%batchSize==0))writer.flush();
                }
                writer.flush();

            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }
}
