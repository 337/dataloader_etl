package com.xingcloud.dataloader.lib;

import com.xingcloud.id.c.IDClient;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.util.Constants;
import com.xingcloud.util.HashFunctions;
import com.xingcloud.xa.uidmapping.UidMappingUtil;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntOpenHashMap;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class SeqUidCacheMapV2 {

  private static final Log LOG = LogFactory.getLog(SeqUidCacheMapV2.class);

  //内存映射到本地的二进制文件每一行的格式为: 8个字节的long+2个字节的'\t'+4个字节的int+2个字节的'\n'
  private static final int ONE_LINE_BYTE = 8 + 4 + 2 * 2;
  //mapper一次读取的行数
  private static final int MAP_BATCH_READ_LINE = 1024;
  private static final int RESET_QUOTA = 300 * 10000;

  //当mysql异常时，sleep时间，单位为ms
  private static final int SLEEP_MILLIS_IF_MYSQL_EXCEPTION = 5000;

  private static SeqUidCacheMapV2 instance;

  private Map<String, LongIntOpenHashMap> map;

  /* Tmp for counting */
  private int missCount;
  private int missCount_govome;
  private int missCount_v9_v9;
  private int missCount_globososo;
  private int missCount_sof_dsk;
  private int missCount_sof_newgdp;
  private int missCount_i18n_status;
  private int missCount_sof_newhpnt;

  private int allCount;
  private int allCount_govome;
  private int allCount_v9_v9;
  private int allCount_globososo;
  private int allCount_sof_dsk;
  private int allCount_sof_newgdp;
  private int allCount_i18n_status;
  private int allCount_sof_newhpnt;

  private Map<String, Long> getUidTime;
  private Map<String, Long> getThriftTime;


  public synchronized static SeqUidCacheMapV2 getInstance() {
    if (instance == null) {
      instance = new SeqUidCacheMapV2();
    }
    return instance;
  }


  private SeqUidCacheMapV2() {
    map = new ConcurrentHashMap<String, LongIntOpenHashMap>();
    getUidTime = new ConcurrentHashMap<String, Long>();
    getThriftTime = new ConcurrentHashMap<String, Long>();
  }


  private void put(String pID, long md5RawUid, int seqUid) {
    LongIntOpenHashMap longIntOpenHashMap = map.get(pID);
    if (longIntOpenHashMap == null) {
      longIntOpenHashMap = new LongIntOpenHashMap();
      map.put(pID, longIntOpenHashMap);

      LOG.info("First time init uid cache for project " + pID);
    }

    longIntOpenHashMap.put(md5RawUid, seqUid);
  }


  private int get(String pID, long md5RawUid) {
    LongIntOpenHashMap longIntOpenHashMap = map.get(pID);
    if (longIntOpenHashMap == null) {
      return 0;
    }
    return longIntOpenHashMap.get(md5RawUid);
  }


  /**
   * for test only
   */
  protected Map<String, LongIntOpenHashMap> getMap() {
    return map;
  }


  /**
   * 把uid cache flush到本地
   */
  public void flushCacheToLocal(String pID) {
    long startTime = System.currentTimeMillis();

    // transform to byte[8]byte[2]byte[4]byte[2]/long\tint\n.
    FileChannel fileChannel = null;
    try {
      LongIntOpenHashMap longIntOpenHashMap = map.get(pID);
      if (longIntOpenHashMap == null)
        return;

      fileChannel = new RandomAccessFile(Constants.UID_CACHE_LOCAL_FILE_PREFIX + pID, "rw").getChannel();
      //每次写到本地磁盘时，清空文件
      fileChannel.truncate(0);

      long fileSize = longIntOpenHashMap.size() * ONE_LINE_BYTE;
      MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);

      final long[] keys = longIntOpenHashMap.keys;
      final int[] values = longIntOpenHashMap.values;
      final boolean[] states = longIntOpenHashMap.allocated;

      for (int i = 0; i < states.length; i++) {
        if (states[i]) {
          buffer.putLong(keys[i]);
          buffer.putChar('\t');
          buffer.putInt(values[i]);
          buffer.putChar('\n');
        }
      }

      // this operation is a little slow
      // more info, please ref to:
      // http://stackoverflow.com/questions/4096881/do-we-need-to-use-mappedbytebuffer-force-to-flush-data-to-disk
//      buffer.force();

      LOG.info("------UIDCACHE " + pID + " -------- flush uid cache completed. Size:" + longIntOpenHashMap.size() +
              " Taken: " + (System.currentTimeMillis() - startTime) + "ms.");
    } catch (IOException e) {
      LOG.error("------UIDCACHE" + pID + "--------- flush uid cache error. " + e.getMessage());
    } finally {
      if (fileChannel != null)
        try {
          fileChannel.close();
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
    }
  }


  /**
   * 从本地恢复uid cache ,如果map中有这个pid，则直接return。
   */
  public void initCache(String pID) {
    if (map.get(pID) != null)
      return;

    long startTime = System.currentTimeMillis();

    File file = new File(Constants.UID_CACHE_LOCAL_FILE_PREFIX + pID);
    FileChannel channel = null;
    LongIntOpenHashMap longIntOpenHashMap = null;

    try {
      channel = new FileInputStream(file).getChannel();
      MappedByteBuffer buff = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

      byte[] b = new byte[MAP_BATCH_READ_LINE * ONE_LINE_BYTE];
      long linesCount = file.length() / ONE_LINE_BYTE;   //文件行数

      longIntOpenHashMap = new LongIntOpenHashMap(Base64Util_Helper.getCapacity(linesCount));

      for (int index = 0; index < linesCount; index += MAP_BATCH_READ_LINE) {
        //一次读取 MAP_BATCH_READ_LINE
        if (linesCount - index < MAP_BATCH_READ_LINE) {
          b = new byte[(int) ((linesCount - index) * ONE_LINE_BYTE)];
        }
        buff.get(b);
        int indeedSize = b.length / ONE_LINE_BYTE;     //实际一次读取的行数
        for (int i = 0; i < indeedSize; i++) {
          int begin = i * ONE_LINE_BYTE;
          long key = Base64Util_Helper.toLong(b, begin);
          long tab = Base64Util_Helper.byteToTab(b, begin+8);
          if (tab != '\t') {
            throw new RuntimeException("Unrecognized file format.");
          }
          int value = Base64Util_Helper.toInteger(b, begin+10);
          longIntOpenHashMap.put(key, value);
        }
      }
      map.put(pID, longIntOpenHashMap);

      LOG.info("------UIDCACHE " + pID + " -------- init cache completed. Size:" + linesCount + " Taken: " +
        (System.currentTimeMillis() - startTime) + "ms.");
    } catch (java.io.FileNotFoundException fne) {
      LOG.warn("------UIDCACHE " + pID + " -------- new pid." + fne.getMessage());
    } catch (Exception e) {
      LOG.error("------UIDCACHE " + pID + " -------- parse uid cache " + Constants.UID_CACHE_LOCAL_FILE_PREFIX
              + pID + " error. " + e.getMessage());
    } finally {
      if (channel != null) {
        try {
          channel.close();
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }


  private long getInnerUidFromMySQL(String project, String rawUid) {
    int tryTimes = 1;

    long innerUid = -1;
    while (innerUid < 0) {
      try {
        innerUid = IDClient.getInstance().getCreatedId(project, rawUid);
        if (innerUid < 0) {
          LOG.error("get inner uid from id service return -1." +
            " project: " + project + ", rawUid: " + rawUid +
            " retry in " + SLEEP_MILLIS_IF_MYSQL_EXCEPTION * tryTimes / 1000 + " seconds.");
        }
      } catch (Exception ex) {
        LOG.error("get inner uid from id service throw exception." +
          " project: " + project + ", rawUid: " + rawUid  +
          " retry in " + SLEEP_MILLIS_IF_MYSQL_EXCEPTION * tryTimes / 1000 +
          " seconds. " + ex.getMessage());
      }

      if (innerUid < 0) {
        try {
          Thread.sleep(SLEEP_MILLIS_IF_MYSQL_EXCEPTION * tryTimes);
          tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
        } catch (InterruptedException ie) {
          break;
        }
      }
    }

    return innerUid;
  }


  /**
   * 从缓存中获取cache的uid
   */
  public int getUidCache(String project, String rawUid) throws Exception {
    long s = System.nanoTime();

    long md5RawUid = HashFunctions.md5(rawUid.getBytes());
    int seqUid = get(project, md5RawUid);
    if (seqUid == 0) {
      long st = System.nanoTime();

      long innerUid = getInnerUidFromMySQL(project, rawUid);
      seqUid = (int)innerUid;

      addThriftTime(project, System.nanoTime() - st);

      if (innerUid < 0) {
        throw new Exception(project + " " + rawUid + " getInnerUidFromMySQL failed.");
      }

      // 保证每个在sequidcachemap里面的uid，是在热表/或者从冷表转到热表/或者2个表都不在（新增的）的uid；
      // flush就不需要在每次flush时候去check这个uid是否需要从冷表转。
      // 本地缓存不在的uid；执行chechInHot和coldtohot
      if (!(project.equals("govome")
        || project.equals("globososo")
        || project.equals("sof-newgdp")
        || project.equals("i18n-status"))) {
        if (!checkInHot(project, innerUid)) {
          cold2Hot(project, UidMappingUtil.getInstance().decorateWithMD5(innerUid));
        }
      }

      put(project, md5RawUid, seqUid);
      missCount++;


      if (project.equals("v9-v9"))
        missCount_v9_v9++;
      else if (project.equals("govome"))
        missCount_govome++;
      else if (project.equals("globososo"))
        missCount_globososo++;
      else if (project.equals("sof-dsk"))
        missCount_sof_dsk++;
      else if (project.equals("sof-newgdp"))
        missCount_sof_newgdp++;
      else if (project.equals("i18n-status"))
        missCount_i18n_status++;
      else if (project.equals("sof-newhpnt"))
        missCount_sof_newhpnt++;

    }

    addGetUidTime(project, System.nanoTime() - s);
    allCount++;


    if (project.equals("v9-v9"))
      allCount_v9_v9++;
    else if (project.equals("govome"))
      allCount_govome++;
    else if (project.equals("globososo"))
      allCount_globososo++;
    else if (project.equals("sof-dsk"))
      allCount_sof_dsk++;
    else if (project.equals("sof-newgdp"))
      allCount_sof_newgdp++;
    else if (project.equals("i18n-status"))
      allCount_i18n_status++;
    else if (project.equals("sof-newhpnt"))
      allCount_sof_newhpnt++;

    return seqUid;
  }


  //如果某个项目的缓存数超过RESET_QUOTA，就重置这个项目。
  public void resetPidCache(String pid) {
    if (map.get(pid) == null)
      return;

    final int size = map.get(pid).size();
    if (size > RESET_QUOTA) {
      LongIntOpenHashMap shrinkMap = map.get(pid);
      final long[] keys = shrinkMap.keys;
      final boolean[] states = shrinkMap.allocated;
      LongArrayList delKeys = new LongArrayList();
      for (int i = 0, j = 0; i < states.length && j < size - RESET_QUOTA; i++) {
        if (states[i]) {
          delKeys.add(keys[i]);
          j++;
        }
      }
      shrinkMap.removeAll(delKeys);
      LOG.info("------UIDCACHE " + pid + " -------- reset cache completed. " +
        "orig size: " + size + ", now is: " + shrinkMap.size());
    }
  }


  private void addGetUidTime(String project, long timeUsed) {
    Long time = getUidTime.get(project);
    if (time == null)
      time = 0l;
    time += timeUsed;
    getUidTime.put(project, time);
  }


  private void addThriftTime(String project, long timeUsed) {
    Long time = getThriftTime.get(project);
    if (time == null) {
      time = 0l;
    }
    time += timeUsed;
    getThriftTime.put(project, time);
  }


  public long oneReadTaskGetUidNanoTime(String project) {
    return getUidTime.get(project) == null ? 0 : getUidTime.get(project);
  }


  public void printStats() {
    long hitRatio = 0;
    if (allCount != 0) {
      hitRatio = 100 - missCount * 100 / allCount;
    }

    LOG.info("------UIDCACHE-------- 5min task cache allCount: " + allCount + " ;hitCount: " + (allCount - missCount) + " ;hitRatio: " + hitRatio + "%");

    long v9_v9_UidTime = getUidTime.get("v9-v9") == null ? 0 : getUidTime.get("v9-v9");
    long govome_UidTime = getUidTime.get("govome") == null ? 0 : getUidTime.get("govome");
    long globososo_UidTime = getUidTime.get("globososo") == null ? 0 : getUidTime.get("globososo");
    long sof_dsk_UidTime = getUidTime.get("sof-dsk") == null ? 0 : getUidTime.get("sof-dsk");
    long sof_newgdp_UidTime = getUidTime.get("sof-newgdp") == null ? 0 : getUidTime.get("sof-newgdp");
    long i18n_status_UidTime = getUidTime.get("i18n-status") == null ? 0 : getUidTime.get("i18n-status");
    long sof_newhpnt_UidTime = getUidTime.get("sof-newhpnt") == null ? 0 : getUidTime.get("sof-newhpnt");

    long v9_v9_ThriftTime = getThriftTime.get("v9-v9") == null ? 0 : getThriftTime.get("v9-v9");
    long govome_ThriftTime = getThriftTime.get("govome") == null ? 0 : getThriftTime.get("govome");
    long globososo_ThriftTime = getThriftTime.get("globososo") == null ? 0 : getThriftTime.get("globososo");
    long sof_dsk_ThriftTime = getThriftTime.get("sof-dsk") == null ? 0 : getThriftTime.get("sof-dsk");
    long sof_newgdp_ThriftTime = getThriftTime.get("sof-newgdp") == null ? 0 : getThriftTime.get("sof-newgdp");
    long i18n_status_ThriftTime = getThriftTime.get("i18n-status") == null ? 0 : getThriftTime.get("i18n-status");
    long sof_newhpnt_ThriftTime = getThriftTime.get("sof-newhpnt") == null ? 0 : getThriftTime.get("sof-newhpnt");


    LOG.info("------UIDCACHE-------- getUid v9-v9:" + v9_v9_UidTime / 1000000 + "ms.govome:" +
            govome_UidTime / 1000000 + "ms.globososo:" + globososo_UidTime / 1000000 + "ms" +
            ".sof_dsk:" + sof_dsk_UidTime / 1000000 + "ms.sof-newgdp:" + sof_newgdp_UidTime / 1000000 + "ms" +
            ".i18n_statu:" + i18n_status_UidTime / 1000000 + "ms.sof_newhpnt:" + sof_newhpnt_UidTime / 1000000 + "ms.");
    LOG.info("------UIDCACHE-------- getThrift v9-v9:" + v9_v9_ThriftTime / 1000000 + "ms.govome:" +
            govome_ThriftTime / 1000000 + "ms.globososo:" + globososo_ThriftTime / 1000000 + "ms" +
            ".sof_dsk:" + sof_dsk_ThriftTime / 1000000 + "ms.sof-newgdp:" + sof_newgdp_ThriftTime / 1000000 + "ms" +
            ".i18n_statu:" + i18n_status_ThriftTime / 1000000 + "ms.sof_newhpnt:" + sof_newhpnt_ThriftTime / 1000000 + "ms.");

    LOG.info("------UIDCACHE-------- cache hit v9-v9: allCount" + allCount_v9_v9 + " hitCount:" + (allCount_v9_v9 - missCount_v9_v9)
            + " Total cached uid number: " + (map.get("v9-v9")==null?0:map.get("v9-v9").size()));
    LOG.info("------UIDCACHE-------- cache hit govome: allCount: " + allCount_govome + " hitCount: " +
            (allCount_govome - missCount_govome) + " Total cached uid number: " + (map.get("govome")==null?0:map.get("govome").size()));
    LOG.info("------UIDCACHE-------- cache hit globososo: allCount: " + allCount_globososo + " hitCount: " +
            (allCount_globososo - missCount_globososo)
            + " Total cached uid number: " + (map.get("globososo")==null?0:map.get("globososo").size()));
    LOG.info("------UIDCACHE-------- cache hit sof_dsk: allCount: " + allCount_sof_dsk + " hitCount: " +
            (allCount_sof_dsk - missCount_sof_dsk)
            + " Total cached uid number: " + (map.get("sof-dsk")==null?0:map.get("sof-dsk").size()));
    LOG.info("------UIDCACHE-------- cache hit sof-newgdp: allCount: " + allCount_sof_newgdp + " hitCount: " +
            (allCount_sof_newgdp - missCount_sof_newgdp)
            + " Total cached uid number: " + (map.get("sof-newgdp")==null?0:map.get("sof-newgdp").size()));
    LOG.info("------UIDCACHE-------- cache hit i18n_status: allCount: " + allCount_i18n_status + " hitCount: " +
            (allCount_i18n_status - missCount_i18n_status)
            + " Total cached uid number: " + (map.get("i18n-status")==null?0:map.get("i18n-status").size()));
    LOG.info("------UIDCACHE-------- cache hit sof_newhpnt: allCount: " + allCount_sof_newhpnt + " hitCount: " +
            (allCount_sof_newhpnt - missCount_sof_newhpnt)
            + " Total cached uid number: " + (map.get("sof-newhpnt")==null?0:map.get("sof-newhpnt").size()));

  }


  public void resetStats() {
    allCount = 0;
    missCount = 0;

    allCount_v9_v9 = 0;
    allCount_sof_dsk = 0;
    allCount_govome = 0;
    allCount_globososo = 0;
    allCount_sof_newgdp = 0;
    allCount_i18n_status = 0;
    allCount_sof_newhpnt = 0;

    missCount_v9_v9 = 0;
    missCount_govome = 0;
    missCount_globososo = 0;
    missCount_sof_dsk = 0;
    missCount_sof_newgdp = 0;
    missCount_i18n_status = 0;
    missCount_sof_newhpnt = 0;

    getUidTime.clear();
    getThriftTime.clear();
  }


  private boolean checkInHot(String project, long innerUid) {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    long samplingUid = UidMappingUtil.getInstance().decorateWithMD5(innerUid);
    int tryTimes = 1;
    boolean returnValue = false;
    boolean successful = false;
    while (!successful){
      try {
        // for each retry, initialize these variables to null
        connection = null; statement = null; resultSet = null;
        connection = getUidConn(project, innerUid);
        statement = connection.createStatement();
        resultSet = statement.executeQuery("select uid from `register_time` where uid=" + samplingUid);

        returnValue = resultSet.next();
        successful = true;
      } catch (SQLException sqle) {
        LOG.error("check hot uid failed." +
          " project: " + project + ", inner uid: " + innerUid +
          " retry in " + SLEEP_MILLIS_IF_MYSQL_EXCEPTION * tryTimes / 1000 +
          " seconds. " + sqle.getMessage());
      } finally {
        DbUtils.closeQuietly(connection, statement, resultSet);
      }

      if (!successful) {
        try {
          Thread.sleep(SLEEP_MILLIS_IF_MYSQL_EXCEPTION * tryTimes);
          tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
        } catch (InterruptedException ie) {
          successful = true;
        }
      }
    }

    return returnValue;
  }


  private void cold2Hot(String project, long samplingUid) {
    int tryTimes = 1;

    boolean successful = false;
    while (!successful){
      try {
        MySql_16seqid.getInstance().cold2hot(project, samplingUid);
        successful = true;
      } catch (SQLException sqle) {
        LOG.error("cold2hot failed. " +
          " project: " + project + ", sampling uid: " + samplingUid +
          " retry in " + SLEEP_MILLIS_IF_MYSQL_EXCEPTION * tryTimes / 1000 +
          " seconds. " + sqle.getMessage());
      }

      if (!successful) {
        try {
          Thread.sleep(SLEEP_MILLIS_IF_MYSQL_EXCEPTION * tryTimes);
          tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
        } catch (InterruptedException ie) {
          successful = true;
        }
      }
    }
  }


  private Connection getUidConn(String project, long innerUid) throws SQLException {
    String nodeAddress = UidMappingUtil.getInstance().hash(innerUid);
    return MySql_16seqid.getInstance().getConnByNode(project, nodeAddress);
  }
}
