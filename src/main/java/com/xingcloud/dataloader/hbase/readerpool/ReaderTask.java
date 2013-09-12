package com.xingcloud.dataloader.hbase.readerpool;

import com.xingcloud.dataloader.hbase.table.user.UserPropertyBitmaps;
import com.xingcloud.dataloader.hbase.tableput.CombineTablePut;
import com.xingcloud.dataloader.hbase.tableput.TablePut;
import com.xingcloud.dataloader.hbase.tableput.UserTablePut;
import com.xingcloud.dataloader.lib.*;
import com.xingcloud.util.Constants;
import com.xingcloud.util.ProjectInfo;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 读取一个项目的所有的服id下的日志，将要上报的数据放到tabluput里面，
 * Author: qiujiawei
 * Date:   12-6-25
 */
public class ReaderTask implements Runnable {
  public static final Log LOG = LogFactory.getLog(ReaderTask.class);
  private String project;
  private List<String> appidList;
  private TablePut tablePut;

  private String date;
  private int index;

  private long timeTotal;

  private int flushInternal = 48;

  private List<String> v4Logs;

  //coint  batchCoinEventNum
  int batchCoinEventNum = 1000;

  /**
   * 构建读取任务
   *
   * @param project   要读取的项目
   * @param appidList 要读取的项目对于的服列表
   * @param tablePut  上报数据的存储对象
   * @param date      处理的日期
   * @param index     处理的5分钟时间段
   */


  public ReaderTask(String project, List<String> appidList, TablePut tablePut, String date, int index, List<String> v4Logs) {
    this.project = project;
    this.appidList = appidList;
    this.tablePut = tablePut;

    this.date = date;
    this.index = index;

    this.v4Logs = v4Logs;
  }

  public void run() {
    try {

      long t1 = System.currentTimeMillis();
      List<Long> timeList = new ArrayList<Long>();
      //每天第一个任务，从mysql dump 最近60天的访问用户，这部分用户的ref属性（ref，ref0-ref4）不再重复更新
      //如果某个用户前一天新注册但是没有在那一天内更新ref*属性，那他的ref*属性就一直不能更新
      //重启时候，会从本地文件恢复ref的bitmap
      //其他情况，这个ref不做更新
      //TODO
//      RefBitMapRebuild.getInstance().rebuildSixtyDays(project, date, index);

      SeqUidCacheMap.getInstance().initCache(project);

      int processCoinThreadNum = 1;
      ExecutorService audit_exec = Executors.newFixedThreadPool(processCoinThreadNum);


      //轮询所有服id分析sitedata日志和store_log日志
      for (String appid : appidList) {
        if (appid.equals("happyranch@facebook_ar_1") || appid.equals("happyranch@gg_pl_1")) {
          timeList.add(0l);
          continue;
        }
        long tt1 = System.currentTimeMillis();
        LOG.info("begin to process " + appid);
        processAllFile(audit_exec, tablePut, LocalPath.SITE_DATA, appid, date, index);
        processAllFile(audit_exec, tablePut, LocalPath.STORE_LOG, appid, date, index);
        LOG.info("process " + appid + "completely!");
        long tt2 = System.currentTimeMillis();
        timeList.add(tt2 - tt1);
      }

      //process v4 log
      processV4Log(audit_exec, tablePut, LocalPath.V4_LOG, date, index);

      //wait coin task finish
      processCoinTask(audit_exec);

      long t2 = System.currentTimeMillis();
      this.timeTotal = (t2 - t1);

      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < appidList.size(); i++) {
        sb.append("(").append(appidList.get(i)).append(",").append(timeList.get(i)).append(") ");
      }


      long t3 = System.currentTimeMillis();

      tablePut.flushToLocal();


      //每4个小时flush  cache到本地一次，但是内存中的缓存不清空
      if ((index + 1) % flushInternal == 0)
        SeqUidCacheMap.getInstance().flushCacheToLocal(project);

      //每天检查一次该项目的缓存是否需要重置
      if (index == flushInternal * 3.5)
        SeqUidCacheMap.getInstance().resetPidCache(project);

      LOG.info("finish reading and flushing events the project :" + project + " using " + this.timeTotal + " ms," +
              "getid using " + SeqUidCacheMap.getInstance().oneReadTaskGetUidNanoTime(project) / 1000000 + " ms," +
              "flusing event and user using " + (System.currentTimeMillis() - t3) + "ms. with list:" + sb.toString());
    } catch (Exception e) {
      LOG.error(project + " read task error. " + e.getMessage(), e);
    } finally {
      //跑完一个项目，关掉这个项目在SeqUidCacheMap里面的MysqlConnection
      try {
        SeqUidCacheMap.getInstance().closeOneProjectConnection(project);
      } catch (SQLException e) {
        LOG.error(project + "closeOneProjectConnection error.", e);
      }
    }

  }


  /**
   * 由于一个机器上可能有多个存储目录（迁移机房之用），要轮询所有目录读取日志
   *
   * @param tablePut 中间结果集
   * @param type     日志类型
   * @param appid    服id
   * @param date     日期
   * @param index    处理的5分钟时间段
   */
  protected void processAllFile(ExecutorService audit_exec, TablePut tablePut, String type, String appid, String date,
                                int index) {
    if (type.equals(LocalPath.SITE_DATA)) {
      for (String pathPrefix : LocalPath.SITE_DATA_LIST.split(",")) {
        processFileWithPrefix(audit_exec, pathPrefix, tablePut, type, appid, date, index);
      }
    } else if (type.equals(LocalPath.STORE_LOG)) {
      for (String pathPrefix : LocalPath.STORE_LOG_LIST.split(",")) {
        processFileWithPrefix(audit_exec, pathPrefix, tablePut, type, appid, date, index);
      }
    }
  }

  /**
   * 针对指定日志文件，读取并处理
   *
   * @param prefix   文件目录前缀
   * @param tablePut 中间结果集
   * @param type     日志类型
   * @param appid    服id
   * @param date     日期
   * @param index    处理的5分钟时间段
   */
  protected void processFileWithPrefix(ExecutorService exec, String prefix, TablePut tablePut, String type,
                                       String appid, String date, int index) {
    int logs = 0;
    int logSize = 0;
    int eventNumber = 0;
    int successfulEventNumber = 0;
    BufferedReader bufferedReader = null;

    List<Event> coinEventList = new ArrayList<Event>();

    LogParser logParser = new LogParser(type, ProjectInfo.getProjectInfoFromAppidOrProject(appid));

    try {
      //格式 prefix/site_data/${appid}/${year}/${month}/ea_data_${day}/${index}.log
      String path = LocalPath.getPathWithPrefix(prefix, appid, date, index);
      File file = new File(path);
      if (!file.exists()) {
        LOG.debug(path + " not exist");
        return;
      }
      bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
      String line = null;
      while ((line = bufferedReader.readLine()) != null) {
        if (line.length() < 1) continue;
        logs++;
        //按照行读取，每一行的解析工作交给logparser处理
        List<Event> temp = logParser.parse(line);
        if (temp == null) continue;
        //把返回的事件列表，交给tableput处理
        for (Event event : temp) {
          eventNumber++;
          String eventStr = event.getEvent();
          if (eventStr.contains("audit.produce.buy.coin") || eventStr.contains("audit.produce.promotion.coin") ||
                  eventStr.contains("audit.consume.cost")) {
            coinEventList.add(event);
            successfulEventNumber++;
            if (coinEventList.size() >= batchCoinEventNum) {
              List<Event> submitEvents = coinEventList;
              exec.submit(new CoinProcessTask(project, submitEvents, tablePut));
              coinEventList = new ArrayList<Event>();
            }
          } else if (tablePut.put(event)) successfulEventNumber++;
        }
      }
      LOG.info(appid + " successfulEventNum is " + successfulEventNumber);
    } catch (Exception e) {
      LOG.error("processFile catch Exception " + appid + " " + date + " " + index, e);
    } finally {
      try {
        if (bufferedReader != null) bufferedReader.close();
        if (coinEventList.size() > 0) {
          exec.submit(new CoinProcessTask(project, coinEventList, tablePut));
        }
      } catch (Exception e) {
        LOG.error("close bufferreader catch Exception " + appid + " " + date + " " + index, e);
      }
    }
  }

  private void processV4Log(ExecutorService exec, TablePut tablePut, String type, String date, int index) {
    LogParser logParser = new LogParser(type);
    List<Event> coinEventList = new ArrayList<Event>();

    for (String line : v4Logs) {
      List<Event> events = logParser.parse(line);
      for (Event event : events) {
        String eventStr = event.getEvent();
        if (eventStr.contains("audit.produce.buy.coin") || eventStr.contains("audit.produce.promotion.coin") ||
                eventStr.contains("audit.consume.cost")) {
          coinEventList.add(event);

          if (coinEventList.size() >= batchCoinEventNum) {
            List<Event> submitEvents = coinEventList;
            //TODO
//            exec.submit(new CoinProcessTask(project, submitEvents, tablePut));
            coinEventList = new ArrayList<Event>();
          }
        } else
          tablePut.put(event);
      }
    }
    if (coinEventList.size() > 0) {
//      exec.submit(new CoinProcessTask(project, coinEventList, tablePut));
    }
  }

  private void processCoinTask(ExecutorService exec) throws InterruptedException {
    exec.shutdown();
    boolean result = exec.awaitTermination(ReaderPool.ThreadPoolMaxTime, TimeUnit.SECONDS);
    if (!result) {
      exec.shutdownNow();
      LOG.error(project + "\t" + date + "\t" + index + " Coin Task timeout.");
    }

  }
}
