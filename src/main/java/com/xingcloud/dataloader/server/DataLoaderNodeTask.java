package com.xingcloud.dataloader.server;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
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
import com.xingcloud.util.ProjectInfo;
import com.xingcloud.util.manager.CurrencyManager;
import com.xingcloud.util.manager.NetManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

import java.io.File;
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
    try {
      String publicIp = NetManager.getPublicIp();
      String siteIp = NetManager.getSiteLocalIp();
      LOG.info("begin run Task [" + date + "," + index + "," + runType + ","
              + taskPriority + "] ********************************");


      //存储mongodb启动记录
      DBObject beginObject = new BasicDBObject();
      beginObject.put("public_ip", publicIp);
      beginObject.put("inet_ip", siteIp);
      beginObject.put("date", date);
      beginObject.put("index", index);
      beginObject.put("stats", "begin");


      MongodbDriver.getInstanceDB().getCollection(taskFinishInfoColl).insert(beginObject);


      long t1 = System.currentTimeMillis();

      TablePutPool tablePutPool = new TablePutPool(runType);
      LOG.info("begin to build all table");


      //分配appid到project的关系，并筛选要运行的项目。
      //扫描本地log目录，得到有新log的project名字，并组成 projectid => list<appid>

      Map<String, List<String>> projectAppidMatch = assignAppid();
      LOG.info("assign all appids using " + (System.currentTimeMillis() - t1) + "ms.");


      //12的倍数次项目清空bitmap的lastlogintime
      if (index % 48 == 0) {
        LOG.info("reset bitmap :" + User.lastLoginTimeField);
        for (String project : projectAppidMatch.keySet())
          UserPropertyBitmaps.getInstance().resetPropertyMap(project, User.lastLoginTimeField);
      }

      //48的倍数清空 platformField，  versionField，identifierField，languageField
      if (index  == 5) {
        String[] resetProperties = new String[]{User.platformField, User.versionField, User.identifierField,
                User.languageField};
        for (String property : resetProperties) {
          LOG.info("reset bitmap :" + property);
          for (String project : projectAppidMatch.keySet())
            UserPropertyBitmaps.getInstance().resetPropertyMap(project, property);
        }
      }


      //读取所有的日志并存储到中间类TablePut中去,并flush到本地
      boolean readerFinish = buildProjectTablePut(tablePutPool, projectAppidMatch);

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
      SeqUidCacheMap.getInstance().printStats();
      SeqUidCacheMap.getInstance().resetStats();

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
  private boolean buildProjectTablePut(TablePutPool tablePutPool, Map<String, List<String>> projectAppidMatch)
          throws Exception {
    ReaderPool readerPool = new ReaderPool();
    for (String project : projectAppidMatch.keySet()) {

      TablePut tablePut = tablePutPool.getTablePut(project, date, index);
      ReaderTask readerTask = new ReaderTask(project, projectAppidMatch.get(project), tablePut, date, index);
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
        for (File file : fileList) {
          if (file.isDirectory()) {
            appidSet.add(file.getName());
          }
        }
      }
    }
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
    DataLoaderNodeTask dataLoaderNodeTask = new DataLoaderNodeTask(args[0], Integer.valueOf(args[1]), args[2],
            args[3], "High");
    dataLoaderNodeTask.run();
  }
}