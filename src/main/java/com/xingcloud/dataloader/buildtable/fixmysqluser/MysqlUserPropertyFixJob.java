package com.xingcloud.dataloader.buildtable.fixmysqluser;

import com.xingcloud.dataloader.buildtable.buildmysqltable.HotColdMultipleTextOutputFormat;
import com.xingcloud.dataloader.buildtable.buildmysqltable.UserPropertyMapper;
import com.xingcloud.dataloader.buildtable.buildmysqltable.UserPropertyReducer;
import com.xingcloud.dataloader.buildtable.mapreduce.BuildUtil;
import com.xingcloud.dataloader.lib.HdfsPath;
import com.xingcloud.dataloader.lib.TimeIndex;
import com.xingcloud.dataloader.pool.ThreadPool;
import com.xingcloud.util.Log4jProperties;
import com.xingcloud.xa.hash.HashUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Author: qiujiawei & ivytang
 * Date:   12-6-28
 */
public class MysqlUserPropertyFixJob extends Thread {

    enum functionType {
        HAOOPTOLOCAL, LOCALTOMYSQL, HADOOPTOMYSQL;
    }

    public static final Log LOG = LogFactory.getLog(MysqlUserPropertyFixJob.class);

    private static String MYSQL_PORT = "3306";
    private static String MYSQL_USER = "xingyun";
    private static String MYSQL_PWD = "Ohth3cha";

    private String pathPrefix = HdfsPath.hdfsRoot + "/user/hadoop/fixmysqltable/";

    private String hotDataFile = "TBL_HOT";
    //顺序不能乱,分发raw数据文件到新版本每个属性一个表的格式，对应一个属性一个文件。
    private String[] outputFileNames = {"register_time", "last_login_time", "first_pay_time", "last_pay_time", "grade", "game_time", "pay_amount", "language", "version", "platform", "identifier", "ref"};
    //与时间相关的属性格式发生变化，这些属性的值需要特殊处理
    private String[] needConvertTimePropertiesArray = {"register_time", "last_login_time", "first_pay_time", "last_pay_time"};
    private List<String> needConvertTimeProperties = Arrays.asList(needConvertTimePropertiesArray);


    private String coldDataFile = "TBL_COLD";
    private String coldTableName = "cold_user_info";

    private String project;
    private String beginDate;
    private String endDate;
    private boolean hadoopToLocal;
    private boolean localToMysql;
    private List<String> nodeips;

    public MysqlUserPropertyFixJob(String pID, String startDate, String endDate, boolean hadoopToLocal, boolean localtoMysql) {
        this.project = pID;
        this.beginDate = startDate;
        this.endDate = endDate;
        this.hadoopToLocal = hadoopToLocal;
        this.localToMysql = localtoMysql;
        this.nodeips = HashUtil.getInstance().nodes();

    }

    static public void main(String[] args) {
        Log4jProperties.init();

        String beginProject = null;
        if (args.length > 0 && args[0] != null) {
            long start = System.currentTimeMillis();
            Set<String> projects;
            if (args[0].equals("all") || args[0].startsWith("begin:")) {
                projects = BuildUtil.getProjectList();
            } else {
                projects = new HashSet<String>();
                projects.add(args[0]);
            }

            if (args[0].startsWith("begin")) {
                beginProject = args[0].substring(6);
            }
            String beginDate = TimeIndex.getDate();
            String endDate = TimeIndex.getDate();
            if (args.length > 3) {
                beginDate = args[1];
                endDate = args[2];
                if (beginDate.length() != 10 || endDate.length() != 10) {
                    LOG.info("date should be format yyyy-MM-dd");
                    return;
                }
            } else {
                LOG.info("you should input the begin and end date");
                return;
            }

            boolean hadoopToLocal = false;
            boolean localToMysql = false;
            if (args.length >= 4) {
                if (args[3].equals("hadooptolocal")) {
                    hadoopToLocal = true;
                    localToMysql = false;
                } else if (args[3].equals("localtomysql")) {
                    hadoopToLocal = false;
                    localToMysql = true;

                } else if (args[3].equals("hadooptomysql")) {
                    hadoopToLocal = true;
                    localToMysql = true;
                } else {
                    LOG.error("error");
                }
            }

            List<String> runList = new ArrayList<String>();
            for (String project : projects) {
                if (beginProject == null || project.compareTo(beginProject) >= 0) {
                    runList.add(project);
                }
            }
            LOG.info("ready to run:" + runList);
            int taskNum = 0;
            for (String project : runList) {
                LOG.info("Add task: " + project + " " + beginDate + " " + endDate + " " + hadoopToLocal + " " + localToMysql);
                MysqlUserPropertyFixJob userPropertyFixJob = new MysqlUserPropertyFixJob(project, beginDate, endDate, hadoopToLocal, localToMysql);
                ThreadPool.addTask(userPropertyFixJob);
                taskNum++;
            }
            LOG.info("Total task number: " + taskNum);
            ThreadPool.shutDownAllTasks();
            long end = System.currentTimeMillis();
            LOG.info("all job finish using" + (end - start) / 1000);
        }
    }


    public void run() {
        LOG.info("run:" + project + " " + beginDate + " " + endDate + " " + hadoopToLocal + " " + localToMysql);
        String LocalOutputDir = "/data/dataloader/fix/" + project + "/";
        try {
            if (hadoopToLocal) {
                Path output = getOutputPath(project);
                job(project, beginDate, endDate, output);

                //Clean local file environment.
                File localFile = new File(LocalOutputDir);
                if (!localFile.exists()) {
                    localFile.mkdirs();
                } else {
                    File[] dataFiles = localFile.listFiles();
                    for (File dataFile : dataFiles)
                        dataFile.delete();
                }
                //Move hdfs data file to local.
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);

                Path hotSrcPath = new Path(output, hotDataFile);
                Path coldSrcPath = new Path(output, coldDataFile);
                if ((fs.exists(hotSrcPath))) {
                    fs.moveToLocalFile(hotSrcPath, new Path(LocalOutputDir));
                } else {
                    LOG.warn(project + " has no " + hotSrcPath.toString());
                }
                if (fs.exists(coldSrcPath)) {
                    fs.moveToLocalFile(coldSrcPath, new Path(LocalOutputDir));
                } else {
                    LOG.warn(project + " has no " + coldSrcPath.toString());
                }
//                fs.close();
                LOG.info("Hadoop To Local finished! " + project);
            }
            if (localToMysql) {
                loadDataInfile(project, LocalOutputDir);
            }
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    static String[] getAllProject(String allProject) {
        return allProject.split(",");
    }

    public void job(String project, String beginDate, String endDate, Path output) {
        try {
            Configuration conf = new Configuration();

            Job job = new Job(conf, "user:" + project);


            job.getConfiguration().set("begin", beginDate);
            job.getConfiguration().set("end", endDate);


            job.setJarByClass(MysqlUserPropertyFixJob.class);

            job.setMapperClass(UserPropertyMapper.class);
            job.setReducerClass(UserPropertyReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setInputFormatClass(TextInputFormat.class);
//            job.setOutputFormatClass(TextOutputFormat.class);
            job.setOutputFormatClass(HotColdMultipleTextOutputFormat.class);

            boolean add = false;
            for (Path path : BuildUtil.getUserCollPath(project)) {
                add = true;
                LOG.info("Add log " + path.toString());
                FileInputFormat.addInputPath(job, path);
            }
            FileOutputFormat.setOutputPath(job, output);

            if (add) {
                boolean result = job.waitForCompletion(true);
                if (result)
                    System.out.println(project + " mr build finish");
                else
                    System.out.println(project + " mr build error");
            }
        } catch (Exception e) {
            LOG.error(e);
        }
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
     * @param projectId
     * @param localPathList
     * @author Z J Wu@2012-06-19
     */
    private void loadHotDataFromFile(String projectId,
                                     List<String> localPathList) {

    }

    /**
     * @param projectId
     * @param localPathList
     * @author Z J Wu@2012-06-19
     */
    private void loadColdDataFromFile(String projectId,
                                      List<String> localPathList) {

    }


    /**
     * load the local user file into mysql
     *
     * @param project  the project
     * @param localDir the localdir
     */
    public void loadDataInfile(String project, String localDir) throws Exception {
        splitHotPropertyTable(localDir, hotDataFile);
        rebuildColData(localDir, coldDataFile);

        loadToRemoteMysql(project, localDir);
    }

    /**
     * 将从hdfs里导出的热表数据文件分成每个property一张表一个数据文件的格式。
     *
     * @param parentPath    原始数据文件和拆分后的文件目录
     * @param inputFileName 原始数据文件名称
     * @return
     */
    private void splitHotPropertyTable(String parentPath, String inputFileName) throws Exception {

        BufferedReader inputReader = null;
        Map<String, BufferedWriter[]> outputWriters = new HashMap<String, BufferedWriter[]>();

        try {
            inputReader = new BufferedReader(new FileReader(parentPath + inputFileName));
            for (String node : this.nodeips) {
                BufferedWriter[] bws = new BufferedWriter[outputFileNames.length];
                for (int index = 0; index < outputFileNames.length; index++) {
                    bws[index] = new BufferedWriter(new FileWriter(parentPath + outputFileNames[index] + "_" + node));
                }
                outputWriters.put(node, bws);
            }
            String tempString = null;

            while ((tempString = inputReader.readLine()) != null) {

                String[] items = tempString.split("\t");
                if (items.length < outputFileNames.length + 1) {
                    LOG.error("Wrong data file." + parentPath);
                    return;
                }
                for (int index = 0; index < outputFileNames.length; index++) {
                    String item = items[index + 1];
                    if ("null".equals(item)) continue;

                    if (needConvertTimeProperties.contains(outputFileNames[index]))
                        item = item.replace("-", "") + "000000";
                    String nodeip = HashUtil.getInstance().hash(items[0]);
                    outputWriters.get(nodeip)[index].write(items[0] + "\t" + item + '\n');
                }
            }
            for (Map.Entry<String, BufferedWriter[]> entry : outputWriters.entrySet()) {
                BufferedWriter[] bws = entry.getValue();
                for (BufferedWriter bw : bws)
                    bw.flush();
            }
            LOG.info("Split hot property data every file succeeds.Project file is: " + parentPath + inputFileName);
        } catch (FileNotFoundException e) {
            LOG.warn(parentPath + inputFileName + " doesn't exist.");
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw e;
        } finally {
            if (inputReader != null)
                inputReader.close();
            for (Map.Entry<String, BufferedWriter[]> outputWriter : outputWriters.entrySet()) {
                BufferedWriter[] bws = outputWriter.getValue();
                for (BufferedWriter bw : bws) {
                    if (bw != null)
                        bw.close();
                }

            }
        }
    }


    /**
     *  将冷表的数据格式转换为可以直接load data的格式。
     *
     * @param parentPath   原始冷表数据文件路径
     * @param coldFileName 冷表文件名
     * @throws Exception 转化发生错误
     */

    private void rebuildColData(String parentPath, String coldFileName) throws Exception {
        BufferedReader inputReader = null;
        Map<String, BufferedWriter> outputWriters = new HashMap<String, BufferedWriter>();
        try {
            inputReader = new BufferedReader(new FileReader(parentPath + coldFileName));
            for (String node : this.nodeips) {
                outputWriters.put(node, new BufferedWriter(new FileWriter(parentPath + coldTableName + "_" + node)));
            }
            String tempString = null;

            while ((tempString = inputReader.readLine()) != null) {
                String[] items = tempString.split("\t");
                if (items.length < outputFileNames.length + 1) {
                    LOG.error("Wrong cold data file." + parentPath);
                    return;
                }
                String uid = items[0];
                StringBuilder sb = new StringBuilder();
                sb.append(uid);
                sb.append("\t");
                for (int index = 0; index < outputFileNames.length; index++) {
                    String propertyName = outputFileNames[index];
                    String propertyValue = items[index + 1];
                    if ("null".equals(propertyValue)) continue;
                    if (needConvertTimeProperties.contains(propertyName))
                        propertyValue = propertyValue.replace("-", "") + "000000";
                    sb.append(propertyName);
                    sb.append("=");
                    sb.append(propertyValue);
                    sb.append("\\\n");
                }
                sb.append("\n");
                outputWriters.get(HashUtil.getInstance().hash(items[0])).write(sb.toString());
            }
            for (Map.Entry<String, BufferedWriter> entry : outputWriters.entrySet()) {
                entry.getValue().flush();
            }
            LOG.info("Rebuild cold data succeeds.Project file is: " + parentPath + coldFileName);
        } catch (FileNotFoundException e) {
            LOG.warn(parentPath + coldFileName + " doesn't exist.");
        } catch (Exception e) {
            throw e;
        } finally {
            if (inputReader != null)
                inputReader.close();
            for (Map.Entry<String, BufferedWriter> outputWriter : outputWriters.entrySet()) {
                if (outputWriter.getValue() != null)
                    outputWriter.getValue().close();
            }
        }
    }


    /**
     * 分发好后的属性数据文件loa到远程的mysql
     *
     * @param project  项目名称，也是数据库名
     * @param localDir 本地数据文件目录
     */
    private void loadToRemoteMysql(String project, String localDir) {
        /* Database's name in Mysql is lowercase*/
        project = project.toLowerCase();
        List<String> existDataFiles = getExistDataFiles(localDir);

        //每个属性的6个mysql同时load
        for (String properyName : existDataFiles) {
            Map<String, LoadToMysqlChildThread> childThreads = new HashMap<String, LoadToMysqlChildThread>();
            for (String nodeip : HashUtil.getInstance().nodes()) {
                LoadToMysqlChildThread thread = new LoadToMysqlChildThread(nodeip, project, localDir, properyName,
                        MYSQL_PORT, MYSQL_USER, MYSQL_PWD);
                childThreads.put(nodeip, thread);
                thread.start();

            }
            for (Map.Entry<String, LoadToMysqlChildThread> entry : childThreads.entrySet()) {
                try {
                    entry.getValue().join();
                } catch (InterruptedException e) {
                    LOG.error("ERROR !!!! recover " + project + ":" + properyName + entry.getKey() + " failed. !!!!");
                }
            }
        }
        LOG.info("Project " + project + " recover succeeds!");
    }


    private List<String> getExistDataFiles(String localDir) {


        List<String> allTableNames = new ArrayList<String>();
        for (String outputFileName : outputFileNames) {
            allTableNames.add(outputFileName);
        }
        allTableNames.add(coldTableName);

        String testNode = HashUtil.getInstance().nodes().get(0);
        List<String> existDataFiles = new ArrayList<String>();
        for (String tableName : allTableNames) {
            File file = new File(localDir + tableName + "_" + testNode);
            if (file.exists())
                existDataFiles.add(tableName);
        }
        existDataFiles.remove("ref");
        return existDataFiles;
    }


}


class LoadToMysqlChildThread extends Thread {

    public static final Log LOG = LogFactory.getLog(LoadToMysqlChildThread.class);

    private String destiMysqlHost = null;
    private String project = null;
    private String localDir = null;
    private String properyName = null;
    private String mysqlPort = null;
    private String mysqlUser = null;
    private String mysqlPwd = null;

    public LoadToMysqlChildThread(String nodeIp, String project, String localDir, String properyName, String mysqlPort,
                                  String mysqlUser, String mysqlPwd) {
        this.destiMysqlHost = nodeIp;
        this.project = project;
        this.localDir = localDir;
        this.properyName = properyName;
        this.mysqlPort = mysqlPort;
        this.mysqlUser = mysqlUser;
        this.mysqlPwd = mysqlPwd;

    }

    public void run() {
        String sqlCMD = "use " + project + ";"
                + "DELETE FROM " + properyName + ";"
                + "LOAD DATA LOCAL INFILE '" + localDir + properyName + "_" + destiMysqlHost + "' INTO TABLE "
                + properyName + ";";
        try {
            Runtime rt = Runtime.getRuntime();
            String cmd = "mysql -h" + destiMysqlHost + " -P" + mysqlPort + " -u"
                    + mysqlUser + " -p" + mysqlPwd + " -e\"" + sqlCMD + "\"";
            String[] cmds = new String[]{"/bin/sh", "-c", cmd};
            Process process = rt.exec(cmds);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String cmdOutput = null;
            while ((cmdOutput = bufferedReader.readLine()) != null)
                LOG.info(cmdOutput);
            int result = process.waitFor();
            if (result != 0)
                LOG.error("ERROR !!!! recover " + project + ":" + properyName + destiMysqlHost + " failed. !!!!" +
                        cmd);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }
}
