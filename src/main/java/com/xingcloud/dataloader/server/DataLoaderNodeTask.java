package com.xingcloud.dataloader.server;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.dataloader.driver.MongodbDriver;
import com.xingcloud.dataloader.etloutput.UserOutput;
import com.xingcloud.dataloader.hbase.readerpool.ReaderPool;
import com.xingcloud.dataloader.hbase.readerpool.ReaderTask;
import com.xingcloud.dataloader.hbase.table.user.UserPropertyBitmaps;
import com.xingcloud.dataloader.hbase.tableput.TablePut;
import com.xingcloud.dataloader.hbase.tableput.TablePutPool;
import com.xingcloud.dataloader.lib.*;
import com.xingcloud.redis.RedisResourceManager;
import com.xingcloud.util.Constants;
import com.xingcloud.util.Log4jProperties;
import com.xingcloud.util.ProjectInfo;
import com.xingcloud.util.manager.CurrencyManager;
import com.xingcloud.util.manager.NetManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;

/**
 * 核心任务：参数（日期，5分钟段，项目列表，任务类型,任务优先级 ）
 * <p/>
 * 类型有3种分别是
 * {@link com.xingcloud.dataloader.lib.RunType}
 * 只更新hbase RunType.EventOnly
 * 只更新mysql RunType.UserOnly
 * 同时更新hbase和mysql RunType.Normal
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class DataLoaderNodeTask implements Runnable, Comparable<DataLoaderNodeTask> {
  public Log LOG = LogFactory.getLog(DataLoaderNodeTask.class);
  protected String projectList;
  private String taskFinishInfoColl = "dataloader_etl";

  /**
   * 允许操作的项目列表。
   * 如果为空，则所有项目都需要执行。
   * 如果不为空，则只有列表中的项目需要执行。
   */
  protected Set<String> permitProjectSet;
  protected String date;
  protected RunType runType;
  protected TaskPriority taskPriority;
  protected int index;

  private Set<String> finishProjectSet = new HashSet<String>();

  /**
   * 构造指定的分析任务
   *
   * @param date        日期
   * @param index       5分钟段
   * @param projectList 项目列表 project1|project2|porject3.. 或者all
   * @param runType     Normal,EventOnly,UserOnly
   */
  public DataLoaderNodeTask(String date, int index, String projectList, String runType, String taskPriority) {
    init(date, index, projectList, runType, taskPriority);
  }

  /**
   * 初始化核心任务
   *
   * @param date        日期（yyyymmdd）
   * @param index       5分钟段
   * @param projectList 项目列表 project1|project2|porject3.. 或者all
   * @param runType     Normal,EventOnly,UserOnly
   */
  private void init(String date, int index, String projectList, String runType, String taskPriority) {
    this.date = date;
    this.index = index;
    this.projectList = projectList;
    permitProjectSet = new HashSet<String>();
    if (!projectList.equals("all")) Collections.addAll(permitProjectSet, projectList.split("\\|"));
    this.runType = RunType.valueOf(runType);
    this.taskPriority = TaskPriority.valueOf(taskPriority);
  }


  public void run() {
    //todo: simplify the code
    try {
      String publicIp = NetManager.getPublicIp();
      String siteIp = NetManager.getSiteLocalIp();
      LOG.info("begin run Task [" + date + "," + index + "," + runType + ","
              + taskPriority + "] ********************************");

      //存储mongodb启动记录
      DBObject beginObject = new BasicDBObject();
      //todo: hostname is better than ip address?
      beginObject.put("public_ip", publicIp);
      beginObject.put("inet_ip", siteIp);
      beginObject.put("date", date);
      beginObject.put("index", index);
      beginObject.put("stats", "begin");

      MongodbDriver.getInstanceDB().getCollection(taskFinishInfoColl).insert(beginObject);


      TablePutPool tablePutPool = new TablePutPool(runType);
      LOG.info("begin to build all table");


      //分配appid到project的关系，并筛选要运行的项目。
      //扫描本地log目录，得到有新log的project名字，并组成 projectid => list<appid>
      long t1 = System.currentTimeMillis();
      Map<String, List<String>> projectAppidMatch = assignAppid();
      LOG.info("assign all appids using " + (System.currentTimeMillis() - t1) + "ms.pid size:" + projectAppidMatch.size());

      long t_v4 = System.currentTimeMillis();
      Map<String, List<String>> v4LogsMaps = readV4LogFillV4Task();
      LOG.info("read all v4log using " + (System.currentTimeMillis() - t_v4) + " ms.v4 pid size:" + v4LogsMaps.size());


      //12的倍数次项目清空bitmap的lastlogintime
      if (index % 12 == 0) {
        LOG.info("reset bitmap :" + User.lastLoginTimeField);
        for (String project : projectAppidMatch.keySet())
          UserPropertyBitmaps.getInstance().resetPropertyMap(project, User.lastLoginTimeField);
      }
      //48的倍数清空 platformField，  versionField，identifierField，languageField
      if ((index + 1) % 48 == 0) {
        String[] resetProperties = new String[]{User.platformField, User.versionField, User.identifierField,
                User.languageField};
        for (String property : resetProperties) {
          LOG.info("reset bitmap :" + property);
          for (String project : projectAppidMatch.keySet())
            UserPropertyBitmaps.getInstance().resetPropertyMap(project, property);
        }
      }


      //读取所有的日志并存储到中间类TablePut中去,并flush到本地
      //todo: tablePutPool -> local variable in buildProjectTablePut()
      boolean readerFinish = buildProjectTablePut(tablePutPool, projectAppidMatch, v4LogsMaps);

      long t2 = System.currentTimeMillis();
      LOG.info("using time is :" + (t2 - t1) / 1000 + " second to build all table ");


      MongodbDriver.printAnalytics();
      long t4 = System.currentTimeMillis();
      LOG.info("end run Task [" + date + "," + index + "] using total:" + (t4 - t1) + " build:" + (t2 - t1) + " milliSecond ********************************* ");
      //LOG.info("total using time is :"+(t4-t1)/1000+" second ********************************* ");
      LOG.info("project number is " + finishProjectSet.size());
      LOG.info(finishProjectSet);


      //清除projectinfo缓存，使每次启动task的时候都能更新项目信息

      ProjectInfo.clear();

      //清除CurrencyManager的实例，每次启动task时候都重新初始化，拿到最新的汇率信息
      if (index == Constants.LAST_TASK_NUM)
        CurrencyManager.cleanUp();

      //任务结束后，打印出cache情况,并清空
      SeqUidCacheMapV2.getInstance().printStats();
      SeqUidCacheMapV2.getInstance().resetStats();

      //存储mongodb结束记录
      DBObject finishObject = new BasicDBObject();
      finishObject.put("public_ip", publicIp);
      finishObject.put("inet_ip", siteIp);
      finishObject.put("date", date);
      finishObject.put("index", index);

      if (readerFinish) {
        if (runType == RunType.Normal) finishObject.put("stats", "finish");
        else finishObject.put("stats", "fixfinish");
      } else {
        finishObject.put("stats", "failed");
      }

      finishObject.put("type", runType.name());
      finishObject.put("build", t2 - t1);
      finishObject.put("time", t4 - t1);

      MongodbDriver.getInstanceDB().getCollection(taskFinishInfoColl).insert(finishObject);

      ProjectPropertyCache.clearCache();

      if (index == Constants.LAST_TASK_NUM && taskPriority == TaskPriority.High && projectList.equals("all")) {
        LOG.info("Current day log ends.write user log end .");
        UserOutput.getInstance().writeLogEnd(Constants.USER_DAYEND_LOG);
      }

    } catch (Exception e) {
      LOG.error("dataloader elc run catch exception", e);
    }
  }

  /**
   * 读取所有需要读取的日志。
   * 启动线程池，将每个项目的读取作为一个任务提交到线程池里面。
   * 所有读取出来的数据放在tablePutPool里面。
   *
   * @param tablePutPool      包含所有项目的 tableput
   * @param projectAppidMatch project和appid的映射关系
   * @return 成功或者失败
   */
  private boolean buildProjectTablePut(TablePutPool tablePutPool, Map<String, List<String>> projectAppidMatch, Map<String, List<String>> v4LogsMaps)
          throws Exception {
    ReaderPool readerPool = new ReaderPool();
    Set<String> projectSet = new HashSet<String>();
    projectSet.addAll(projectAppidMatch.keySet());
    projectSet.addAll(v4LogsMaps.keySet());

    //临时忽略对项目gbanner的处理
    LOG.info("temporarily ignore project: gbanner");
    projectSet.remove("gbanner");
    projectSet.remove("newtabv1-bg");
    projectSet.remove("newtabv3-bg");
    projectSet.remove("sof-windowspm");

    for(String pid : StaticConfig.invalidpids){
      projectSet.remove(pid);
    }

    //特殊处理，将处理时间长的项目排在前面
    String[] largeProjects = new String[]{"age","delta-homes","security-protection","22find","sof-installer","searchprotect","sweet-page",
            "sof-wpm","sof-zip","quick-start","sof-ient","sof-isafe","mystartsearch","v9","webssearches","sof-yacnvd","infospace","omiga-plus"};

    for(String project: largeProjects){
      if(projectSet.contains(project)){
        //  TablePut tablePut = tablePutPool.getTablePut(project, date, index);
        List<String> appids = projectAppidMatch.get(project) == null ? new ArrayList<String>() : projectAppidMatch.get(project);
        List<String> v4Logs = v4LogsMaps.get(project) == null ? new ArrayList<String>() : v4LogsMaps.get(project);
        ReaderTask readerTask = new ReaderTask(project, appids, tablePutPool, date, index, v4Logs);
        readerPool.submit(readerTask);
        finishProjectSet.add(project);
        projectSet.remove(project);
      }
    }

    for (String project : projectSet) {
//      TablePut tablePut = tablePutPool.getTablePut(project, date, index);
      List<String> appids = projectAppidMatch.get(project) == null ? new ArrayList<String>() : projectAppidMatch.get(project);
      List<String> v4Logs = v4LogsMaps.get(project) == null ? new ArrayList<String>() : v4LogsMaps.get(project);
      ReaderTask readerTask = new ReaderTask(project, appids, tablePutPool, date, index, v4Logs);
      readerPool.submit(readerTask);
      finishProjectSet.add(project);
    }
    return readerPool.shutdown();
  }


  private List<String> getProjectAppidList(Map<String, List<String>> projectAppidMatch, String project) {
    List<String> list = projectAppidMatch.get(project);

    if (list == null) {
      list = new ArrayList<String>();
      projectAppidMatch.put(project, list);
    }
    return list;
  }

  private Map<String, List<String>> assignAppid() {
    Map<String, List<String>> projectAppidMatch = new HashMap<String, List<String>>();
    for (String appid : getAppidSet(date, index)) {
      ProjectInfo info = ProjectInfo.getProjectInfoFromAppidOrProject(appid);
      LOG.debug(appid + " " + ProjectInfo.getProjectInfoFromAppidOrProject(appid));
      if (info != null && (permitProjectSet.size() == 0 || permitProjectSet.contains(info.getProject()))) {
        getProjectAppidList(projectAppidMatch, info.getProject()).add(appid);
      }
    }
    return projectAppidMatch;
  }


  private Map<String, List<String>> readV4LogFillV4Task() {
    Map<String, List<String>> v4_Logs = new HashMap<String, List<String>>();
    BufferedReader bufferedReader = null;
    try {
      bufferedReader = new BufferedReader(new FileReader(LocalPath.getV4LogPath(LocalPath.V4_LOG_LIST, date, index)));
      String tmpLine = null;
      while ((tmpLine = bufferedReader.readLine()) != null) {
        int appidPos = tmpLine.indexOf("\t");
        if (appidPos > 0) {
          ProjectInfo projectInfo = ProjectInfo.getProjectInfoFromAppidOrProject(tmpLine.substring(0, appidPos));
          if (projectInfo == null)
            continue;
          String project = projectInfo.getProject();
          List<String> pLogs = v4_Logs.get(project);
          if (pLogs == null) {
            pLogs = new LinkedList<String>();
            v4_Logs.put(project, pLogs);
          }
          pLogs.add(tmpLine);
        }
      }
    } catch (FileNotFoundException e) {
      LOG.info("v4log " + date + " " + index + " has not the log.");
    } catch (IOException e) {
      LOG.error("read v4log " + date + " " + index + " error.", e);
    }
    return v4_Logs;
  }

  /**
   * 获取所有可能要运行的appid（服ID）
   *
   * @param date  日期
   * @param index 5分钟时间段
   * @return 包含所有要运行的appid的集合
   */
  private Set<String> getAppidSet(String date, int index) {
    Set<String> appidSet = new HashSet<String>();
    for (String pathPrefix : LocalPath.SITE_DATA_LIST.split(",")) {
      File dir = new File(pathPrefix);
      if (dir.exists()) {
        File[] fileList = dir.listFiles();
        //todo: fileList may be null
        for (File file : fileList) {
          if (file.isDirectory()) {
            appidSet.add(file.getName());
          }
        }
      }

    }

    for (String pathPrefix : LocalPath.STORE_LOG_LIST.split(",")) {
      File dir = new File(pathPrefix);
      if (dir.exists()) {
        File[] fileList = dir.listFiles();
        //todo: fileList may be null
        for (File file : fileList) {
          if (file.isDirectory()) {
            appidSet.add(file.getName());
          }
        }
      }
    }
    LOG.info("local file has appid:" + appidSet.size());



    return appidSet;
  }

  public String toString() {
    return this.date + "," + this.index + "," + this.projectList + "," + this.runType + "," + this.taskPriority;
  }

  public int compareTo(DataLoaderNodeTask o) {
    int pComp = this.taskPriority.compareTo(o.taskPriority);
    if (pComp != 0)
      return pComp;
    else {
      int a = date.compareTo(o.date);
      if (a != 0) return a;
      else return index - o.index;
    }
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DataLoaderNodeTask)) return false;

    DataLoaderNodeTask that = (DataLoaderNodeTask) o;

    if (index != that.index) return false;
    if (!date.equals(that.date)) return false;
    if (!projectList.equals(that.projectList)) return false;
    if (runType != that.runType) return false;
    return taskPriority == that.taskPriority;
  }

  @Override
  public int hashCode() {
    int result = projectList.hashCode();
    result = 31 * result + date.hashCode();
    result = 31 * result + runType.hashCode();
    result = 31 * result + taskPriority.hashCode();
    result = 31 * result + index;
    return result;
  }

  /**
   * main for test
   * args :  String date, int index,String projectList,String runType
   */
  static public void main(String[] args) {
    Log4jProperties.init();
    DataLoaderNodeTask dataLoaderNodeTask = new DataLoaderNodeTask("20130903", 201, "all",
            "Normal", "High");
    dataLoaderNodeTask.run();
  }
}