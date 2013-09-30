package com.xingcloud.dataloader.lib;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.xingcloud.id.c.IDClient;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.util.Constants;
import com.xingcloud.util.HashFunctions;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 12-11-2
 * Time: 下午3:13
 * To change this template use File | Settings | File Templates.
 */
public class SeqUidCacheMap {
  private static Log LOG = LogFactory.getLog(SeqUidCacheMap.class);
  private static final int one_line_byte = 8 + 4 + 2 * 2; //内存映射到本地的二进制文件每一行的格式为 8个字节的long+2个字节的'\t'+4个字节的int+2个字节的'\n'
  private int mapBatchReadLine = 1024 * 100;       //mapper读 一次读取的行数
  private long resetQuota = 400 * 10000l;

  private static SeqUidCacheMap instance;

  private Map<String, LongIntOpenHashMap> map;
  //private Map<String, UidSeqUidCache> map;

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

  private Map<String, Map<String, Connection>> pidNodeConnections;


  public synchronized static SeqUidCacheMap getInstance() {
    if (instance == null) {
      instance = new SeqUidCacheMap();
    }
    return instance;
  }

  private SeqUidCacheMap() {
    map = new ConcurrentHashMap<String, LongIntOpenHashMap>();
    getUidTime = new ConcurrentHashMap<String, Long>();
    pidNodeConnections = new HashMap<String, Map<String, Connection>>();
  }

  private void put(String pID, long md5RawUid, int seqUid) {
    LongIntOpenHashMap longIntOpenHashMap = map.get(pID);
    if (longIntOpenHashMap == null) {
      longIntOpenHashMap = new LongIntOpenHashMap();
      LOG.info("First time init " + pID + " cache.");
      map.put(pID, longIntOpenHashMap);
    }
    longIntOpenHashMap.put(md5RawUid, seqUid);
  }

  private int get(String pID, long md5RawUid) {
    if (!map.containsKey(pID)) {
      return 0;
    }
    LongIntOpenHashMap longIntOpenHashMap = map.get(pID);
    return longIntOpenHashMap.get(md5RawUid);
  }

  /**
   * 把uid cache flush到本地
   *
   * @throws java.io.IOException
   */
  public void flushCacheToLocal(String pID) throws IOException {

    long startTime = System.nanoTime();
    //每次flush到本地是删除之前的文件
    File delFile = new File(Constants.UID_CACHE_LOCAL_FILE_PREFIX + pID);
    if (delFile.exists())
      delFile.delete();

    // transform to byte[8]byte[2]byte[4]byte[2]/long\tint\n.
    FileChannel fileChannel = null;
    try {
      fileChannel = new RandomAccessFile(Constants.UID_CACHE_LOCAL_FILE_PREFIX + pID, "rw").getChannel();
      LongIntOpenHashMap longIntOpenHashMap = map.get(pID);
      if (longIntOpenHashMap == null) return;
      long fileSize = longIntOpenHashMap.size() * one_line_byte;
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
      LOG.info("------UIDCACHE " + pID + " -------- flush uid cache of completed. Size:" + longIntOpenHashMap.size() +
              "Taken: " + (System.nanoTime() - startTime) / 1.0e9 + "s.");
    } catch (IOException e) {
      LOG.error("------UIDCACHE" + pID + "-------- flush uid cache errors." + e.getMessage());
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
   *
   * @throws IOException
   */
  public void initCache(String pID) {
    if (map.get(pID) != null)
      return;
    long startTime = System.nanoTime();
    File file = new File(Constants.UID_CACHE_LOCAL_FILE_PREFIX + pID);
    FileChannel channel = null;
    LongIntOpenHashMap longIntOpenHashMap = new LongIntOpenHashMap();
    try {
      channel = new FileInputStream(file).getChannel();
      MappedByteBuffer buff = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());
      byte[] b;
      long linesCount = file.length() / one_line_byte;   //文件行数
      for (int index = 0; index < linesCount; index += mapBatchReadLine) {
        //一次读取 mapBatchReadLine
        if (linesCount - index > mapBatchReadLine) {
          b = new byte[mapBatchReadLine * one_line_byte];
        } else {
          b = new byte[(int) ((linesCount - index) * one_line_byte)];
        }
        buff.get(b);
        int indeedSize = b.length / one_line_byte;     //实际一次读取的行数
        for (int i = 0; i < indeedSize; i++) {
          int begin = i * one_line_byte;
          long key = Base64Util_Helper.toLong(new byte[]{b[begin], b[begin + 1], b[begin + 2], b[begin + 3], b[begin + 4], b[begin + 5], b[begin + 6], b[begin + 7]});
          char tab = Base64Util_Helper.byteToChar(new byte[]{b[begin + 8], b[begin + 9]});
          if (tab != '\t') {
            throw new RuntimeException("Unrecognized file format.");
          }
          int value = Base64Util_Helper.toInteger(new byte[]{b[begin + 10], b[begin + 11], b[begin + 12], b[begin + 13]});
          longIntOpenHashMap.put(key, value);
        }
      }
      LOG.info("------UIDCACHE " + pID + " -------- init cache completed. Size:" + linesCount + " Taken: " + (System
              .nanoTime() - startTime) / 1.0e9 + "s.");
    } catch (java.io.FileNotFoundException fne) {
      LOG.info("------UIDCACHE " + pID + " -------- new pid.");
    } catch (Exception e) {
      map.remove(pID);
      LOG.error("------UIDCACHE " + pID + " -------- parse uid cache " + Constants.UID_CACHE_LOCAL_FILE_PREFIX
              + pID + " errors.Clear uid cache map" + e.getMessage());
    } finally {
      if (channel != null)
        try {
          channel.close();
        } catch (IOException e) {
          LOG.error(e.getMessage());
        }
    }
    map.put(pID, longIntOpenHashMap);


  }


  /**
   * 从缓存中拿到cache的uid
   *
   * @param project 项目名
   * @param rawUid  rawUid
   * @return
   * @throws Exception
   */
  public int getUidCache(String project, String rawUid) throws Exception {
    long s = System.nanoTime();
    long md5RawUid = HashFunctions.md5(rawUid.getBytes());
    int seqUid = get(project, md5RawUid);
    if (seqUid == 0) {
      seqUid = Integer.parseInt(String.valueOf(IDClient.getInstance().getCreatedId(project, rawUid)));
      if (seqUid < 0) {
        throw new Exception(project + " " + rawUid + " getCreatedId failed");
      }
      //保证每个在sequidcachemap里面的uid，是在热表/或者从冷表转到热表/或者2个表都不在（新增的）的uid；flush_fix就不需要在每次flush时候去check这个uid是否需要从冷表转。
      //本地缓存不在的uid；执行chechInHot和coldtohot
      if (project.equals("govome") || project.equals("globososo")) {
        //do not need cold to host
      } else {
        if (!checkInHot(project, seqUid))
          cold2Hot(project, UidMappingUtil.getInstance().decorateWithMD5(seqUid));

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

  //如果某个项目的缓存数超过resetQuota，就重置这个项目。
  public void resetPidCache(String pid) {
    if (map.get(pid) == null)
      return;
    long size = map.get(pid).size();
    if (size > resetQuota) {
      LongIntOpenHashMap shrinkMap = map.get(pid);
      final long[] keys = shrinkMap.keys;
      final boolean[] states = shrinkMap.allocated;
      LongArrayList delKeys = new LongArrayList();
      for (int i = 0, j = 0; i < states.length && j < size - resetQuota; i++) {
        if (states[i]) {
          delKeys.add(keys[i]);
          j++;
        }
      }
      shrinkMap.removeAll(delKeys);
      LOG.info("------UIDCACHE " + pid + " -------- reset cache completed.orig size: " + size + " .now is :" + map.get
              (pid).size());
    }
  }

  private void addGetUidTime(String project, long timeUsed) {
    Long time = getUidTime.get(project);
    if (time == null)
      time = 0l;
    time += timeUsed;
    getUidTime.put(project, time);
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
    long sof_newgdp_UidTime = getUidTime.get("v9-sof") == null ? 0 : getUidTime.get("v9-sof");
    long i18n_status_UidTime = getUidTime.get("i18n-status") == null ? 0 : getUidTime.get("i18n-status");
    long sof_newhpnt_UidTime = getUidTime.get("sof-newhpnt") == null ? 0 : getUidTime.get("sof-newhpnt");


    LOG.info("------UIDCACHE-------- getUid v9-v9:" + v9_v9_UidTime / 1000000 + "ms.govome:" +
            govome_UidTime / 1000000 + "ms.globososo:" + globososo_UidTime / 1000000 + "ms" +
            ".sof_dsk:" + sof_dsk_UidTime / 1000000 + "ms.v9-sof:" + sof_newgdp_UidTime / 1000000 + "ms" +
            ".i18n_statu:" + i18n_status_UidTime / 1000000 + "ms.sof_newhpnt:" + sof_newhpnt_UidTime / 1000000 + "ms.");
    LOG.info("------UIDCACHE-------- cache hit v9-v9: allCount" + allCount_v9_v9 + " hitCount:" +
            (allCount_v9_v9 - missCount_v9_v9));
    LOG.info("------UIDCACHE-------- cache hit govome: allCount: " + allCount_govome + " hitCount: " +
            (allCount_govome - missCount_govome));
    LOG.info("------UIDCACHE-------- cache hit globososo: allCount: " + allCount_globososo + " hitCount: " +
            (allCount_globososo - missCount_globososo));
    LOG.info("------UIDCACHE-------- cache hit sof_dsk: allCount: " + allCount_sof_dsk + " hitCount: " +
            (allCount_sof_dsk - missCount_sof_dsk));
    LOG.info("------UIDCACHE-------- cache hit v9-sof: allCount: " + allCount_sof_newgdp + " hitCount: " +
            (allCount_sof_newgdp - missCount_sof_newgdp));
    LOG.info("------UIDCACHE-------- cache hit i18n_status: allCount: " + allCount_i18n_status + " hitCount: " +
            (allCount_i18n_status - missCount_i18n_status));
    LOG.info("------UIDCACHE-------- cache hit sof_newhpnt: allCount: " + allCount_sof_newhpnt + " hitCount: " +
            (allCount_sof_newhpnt - missCount_sof_newhpnt));

  }

  public void resetStats() throws SQLException {
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

    getUidTime = new ConcurrentHashMap<String, Long>();
    for (Map.Entry<String, Map<String, Connection>> entry : pidNodeConnections.entrySet()) {
      closeOneProjectConnection(entry.getKey());
    }
  }


  private boolean checkInHot(String project, long innerUid) throws SQLException {
    Connection conn;
    Statement stat = null;
    ResultSet resultSet = null;
    try {
      conn = getUidConn(project, innerUid);
      stat = conn.createStatement();
      resultSet = stat.executeQuery("select uid from `register_time` where uid=" + UidMappingUtil.getInstance().decorateWithMD5(innerUid) + ";");
      return resultSet.next();
    } finally {
      if (resultSet != null)
        resultSet.close();
      if (stat != null)
        stat.close();
    }
  }

  private void cold2Hot(String project, long samplingUid) throws SQLException {
    MySql_16seqid.getInstance().cold2hot(project, samplingUid);
  }

  private Connection getUidConn(String project, long innerUid) throws SQLException {
    Map<String, Connection> nodeConnections = pidNodeConnections.get(project);
    if (nodeConnections == null) {
      nodeConnections = new HashMap<String, Connection>();
      pidNodeConnections.put(project, nodeConnections);
    }
    String nodeAddress = UidMappingUtil.getInstance().hash(innerUid);
    Connection connection = nodeConnections.get(nodeAddress);
    if (connection == null) {
      connection = MySql_16seqid.getInstance().getConnByNode(project, nodeAddress);
      nodeConnections.put(nodeAddress, connection);
    }
    return connection;
  }

  public void closeOneProjectConnection(String project) throws SQLException {
    Map<String, Connection> conections = pidNodeConnections.get(project);
    if (conections != null) {
      pidNodeConnections.remove(project);
      for (Map.Entry<String, Connection> entry : conections.entrySet()) {
        entry.getValue().close();
      }
    }
  }


  public static void main(String[] args) throws Exception {
    if (args[0].equals("stringtobyte")) {
      String stringDir_ = "/home/hadoop/uidcache_etl/";
      String byteDir_ = "/home/hadoop/uidcache_etl_byte/";
//      String stringDir_ = "/Users/ytang1989/Workspace/testfiles/uidcache_etl/";
//      String byteDir_ = "/Users/ytang1989/Workspace/testfiles/uidcache_etl_byte/";

      List<String> pids = new ArrayList<String>();
      File stringDir = new File(stringDir_);
      if (stringDir.isDirectory()) {
        File[] pidFiles = stringDir.listFiles();
        if (pidFiles != null)
          for (File pidFile : pidFiles) {
            if (pidFile.isFile()) {
              pids.add(pidFile.getName());
            }
          }
      }

      for (String pid : pids) {
        System.out.println(pid);

        Map<Long, Integer> caches = new HashMap<Long, Integer>();
        //Read from string format cache
        BufferedReader bufferedReader = null;
        try {
          bufferedReader = new BufferedReader(new FileReader(stringDir_ + pid));
          String tmp = null;
          while ((tmp = bufferedReader.readLine()) != null) {
            int pos = tmp.indexOf("\t");
            if (pos < 0)
              continue;
            caches.put(Long.parseLong(tmp.substring(0, pos)), Integer.parseInt(tmp.substring(pos + 1)));
          }
        } catch (FileNotFoundException e) {
          e.printStackTrace();
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          if (bufferedReader != null)
            try {
              bufferedReader.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
        }

        // transform to byte[8]byte[2]byte[4]byte[2]/long\tint\n.
        FileChannel fileChannel = null;
        try {
          fileChannel = new RandomAccessFile(byteDir_ + pid, "rw").getChannel();
          long fileSize = caches.size() * one_line_byte;
          MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
          for (Map.Entry<Long, Integer> entry : caches.entrySet()) {
            buffer.putLong(entry.getKey());
            buffer.putChar('\t');
            buffer.putInt(entry.getValue());
            buffer.putChar('\n');
          }
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          if (fileChannel != null)
            try {
              fileChannel.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
        }
      }
    } else if (args[0].equals("test")) {
      SeqUidCacheMap seqUidCacheMap = SeqUidCacheMap.getInstance();
      seqUidCacheMap.initCache("sof-newhpnt");
      System.out.println(seqUidCacheMap.get("sof-newhpnt", 254382050115675l));
      System.out.println(seqUidCacheMap.get("sof-newhpnt", 133631773197925l));
      System.out.println(seqUidCacheMap.get("sof-newhpnt", 266009178681034l));
      System.out.println(seqUidCacheMap.get("sof-newhpnt", 199672445542694l));

      seqUidCacheMap.initCache("ivy");
      long seqID1 = seqUidCacheMap.getUidCache("ivy", "1");
      long seqID2 = seqUidCacheMap.getUidCache("ivy", "2");

      System.out.println(seqID1);
      System.out.println(seqID2);

      seqUidCacheMap.flushCacheToLocal("ivy");


    }
  }


}
