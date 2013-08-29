package com.xingcloud.dataloader.lib;

import com.sun.org.apache.xalan.internal.xsltc.dom.SimpleResultTreeImpl;
import com.xingcloud.dataloader.hbase.table.user.UserPropertyBitmaps;
import com.xingcloud.redis.RedisShardedPoolResourceManager;
import com.xingcloud.util.Constants;
import com.xingcloud.util.ProjectInfo;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.*;

/**
 * User: IvyTang
 * Date: 13-7-15
 * Time: PM3:47
 */
public class RefBitMapRebuild {

  private static final Log LOG = LogFactory.getLog(RefBitMapRebuild.class);

  private static final RefBitMapRebuild instance = new RefBitMapRebuild();

  private Set<String> ignoreProjects = new HashSet<String>();

  private String[] canRecoverFromLocalFiles = new String[]{User.refField, User.registerField, User.geoipField, User.nationField};

  private RefBitMapRebuild() {
    ignoreProjects.add("govome");
    ignoreProjects.add("globososo");
  }

  public static RefBitMapRebuild getInstance() {
    return instance;
  }

  public void rebuildSixtyDays(String project, String date, int index) {
    if (ignoreProjects.contains(project)) return;
    if (index == Constants.FIRST_TASK_NUM) {
      rebuildSixtyDaysActiveUsersFromMySQL(project, date);
    } else {
      if (UserPropertyBitmaps.getInstance().ifPropertyNull(project, User.refField)) {
        rebuildSixtyDaysActiveUsersFromLocalFile(project, canRecoverFromLocalFiles);
      }
    }
  }

  private void rebuildSixtyDaysActiveUsersFromMySQL(String project, String date) {
    dumpSixtyDaysActiveUsersToLocal(project, date);
    rebuildSixtyDaysActiveUsersFromLocalFile(project, new String[]{User.refField});
  }

  private void dumpSixtyDaysActiveUsersToLocal(String project, String date) {
    // load 60 active users from mysql to localfile.
    long currentTime = System.currentTimeMillis();
    String[] nodes = UidMappingUtil.getInstance().nodes().toArray(new String[UidMappingUtil.getInstance().nodes()
            .size()]);
//    String[] nodes = new String[]{"192.168.1.147","192.168.1.148","192.168.1.150","192.168.1.152","192.168.1.154",
//            "192.168.1.151"};
    shuffle(nodes);
    for (String mysqlHost : nodes) {
      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS + File.separator + project + "_" + mysqlHost;
      BufferedReader dumpBufferedReader = null;
      try {
        Runtime rt = Runtime.getRuntime();
        String sqlCMD = "mysql -uxingyun -pOhth3cha --database "+getMySQLDBName(project) + " -h" + mysqlHost + " -ss -e\"SELECT" +
                " uid FROM last_login_time where val>=" + TimeIndexV2.getSixtyDaysBefore(date+"000000") + " and val<" +
                TimeIndexV2.getRightHoursBefore(date + "000000") + "\" > " + filePath;
        LOG.info(sqlCMD);
        String[] cmds = new String[]{"/bin/sh", "-c", sqlCMD};
        Process process = rt.exec(cmds);
        dumpBufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String cmdOutput = null;
        while ((cmdOutput = dumpBufferedReader.readLine()) != null)
          LOG.info(cmdOutput);
        int result = process.waitFor();
        if (result != 0)
          throw new RuntimeException("ERROR !!!! get 60 days active user for " + project + " " + mysqlHost + " failed.");
      } catch (Exception e) {
        LOG.error(e.getMessage());
      } finally {
        if (dumpBufferedReader != null)
          try {
            dumpBufferedReader.close();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
      }
    }
    LOG.info(project + " dump  60 active from mysql  using " + (System.currentTimeMillis() - currentTime) + " " +
            "ms.");

  }

  private void rebuildSixtyDaysActiveUsersFromLocalFile(String project, String[] properties) {

    for (String property : properties){
      UserPropertyBitmaps.getInstance().resetPropertyMap(project,property);
      UserPropertyBitmaps.getInstance().initPropertyMap(project, property);
    }

    long currentTime = System.currentTimeMillis();

//    String[] nodes = new String[]{"192.168.1.147","192.168.1.148","192.168.1.150","192.168.1.152","192.168.1.154",
//            "192.168.1.151"};
//    for (String mysqlHost :nodes) {
    for (String mysqlHost : UidMappingUtil.getInstance().nodes()) {
      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS + File.separator + project + "_" + mysqlHost;
      BufferedReader bufferedReader = null;
      try {
        bufferedReader = new BufferedReader(new FileReader(filePath));
        String tmpLine = null;
        while ((tmpLine = bufferedReader.readLine()) != null) {
          long innerUid = Long.parseLong(tmpLine) & 0xffffffffl;
          for (String property : properties)
            UserPropertyBitmaps.getInstance().markPropertyHit(project, innerUid, property);
        }
      } catch (FileNotFoundException e) {
        //do thing
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      } finally {
        if (bufferedReader != null)
          try {
            bufferedReader.close();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
      }
      LOG.info(project+" read "+filePath);
    }
    LOG.info(project + " rebuild ref,nation,register_time,geoip from local file using " + (System.currentTimeMillis()
            - currentTime) + " " + "" + "ms.");
  }


  private void exch(String[] list, int i, int j) {
    String swap = list[i];
    list[i] = list[j];
    list[j] = swap;
  }

  // take as input an array of strings and rearrange them in random order
  private void shuffle(String[] list) {
    int N = list.length;
    Random random = new Random();
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N); // between i and N-1
//      System.out.println(r);
      exch(list, i, r);
    }

  }

  private static String getMySQLDBName(String project){
    return "16_"+project;
  }


  public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException, ParseException {

    String project_ = "sof-dsk";
    String mysqlHost_ = "192.168.1.147";
    String date_="20130808";
    String filePath_ = "/tmp/test";

    String sqlCMD = "mysql -uxingyun -pOhth3cha --database "+getMySQLDBName(project_) + " -h" + mysqlHost_ + " -ss " +
            "-e\"SELECT" +
            " uid FROM last_login_time where val>=" + TimeIndexV2.getSixtyDaysBefore(date_+"000000") + " and val<" +
            TimeIndexV2.getRightHoursBefore(date_+"000000") + "\" > " + filePath_;
    System.out.println(sqlCMD);

    System.out.println(UidMappingUtil.getInstance().nodes());

    if (args.length != 0) {
      if (args[0].equals("test")) {
        //    getInstance().rebuildSixtyDays("sof-newgdp", "20130715", 0);
        getInstance().rebuildSixtyDays("sof-newgdp", "20130715", 4);
        int[] innerUids = new int[]{6781865,
                6617775,
                6843889,
                6379767,
                6607213,
                2025759,
                6881009,
                3733721,
                6814597,
                6905067, 8978181};
        for (int innerUid : innerUids) {
          if (!UserPropertyBitmaps.getInstance().isPropertyHit("sof-newgdp", 8978181, User.ref0Field))
            System.out.println("error " + innerUid);
        }
      } else if (args[0].equals("init")) {
        ShardedJedis shardedJedis = null;
        Set<String> pids = new HashSet<String>();
        try {
          shardedJedis = RedisShardedPoolResourceManager.getInstance().getCache(Constants.REDIS_DB_NUM);
          Collection<Jedis> allShards = shardedJedis.getAllShards();
          for (Jedis jedis : allShards) {
            pids.addAll(jedis.keys("ui.check.*"));
          }
        } catch (Exception e) {
          RedisShardedPoolResourceManager.getInstance().returnBrokenResource(shardedJedis);
          shardedJedis = null;
        } finally {
          RedisShardedPoolResourceManager.getInstance().returnResource(shardedJedis);
        }
        long currentTime = System.currentTimeMillis();
        String nowDate = args[1];
        for (String pid : pids) {
          getInstance().dumpSixtyDaysActiveUsersToLocal(pid.replace("ui.check.", ""), nowDate);
        }
        System.out.println("all finished.using " + (System.currentTimeMillis() - currentTime) / 1000 + "s.");
      } else if (args[0].equals("overflow")) {
        ShardedJedis shardedJedis = null;
        Set<String> pids = new HashSet<String>();
        try {
          shardedJedis = RedisShardedPoolResourceManager.getInstance().getCache(Constants.REDIS_DB_NUM);
          Collection<Jedis> allShards = shardedJedis.getAllShards();
          for (Jedis jedis : allShards) {
            pids.addAll(jedis.keys("ui.check.*"));
          }
        } catch (Exception e) {
          RedisShardedPoolResourceManager.getInstance().returnBrokenResource(shardedJedis);
          shardedJedis = null;
        } finally {
          RedisShardedPoolResourceManager.getInstance().returnResource(shardedJedis);
        }
        for (String pid : pids) {
          System.out.println(pid.replace("ui.check.", ""));
          try {
            getInstance().rebuildSixtyDaysActiveUsersFromLocalFile(pid.replace("ui.check.", ""), new String[]{"ref"});
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    } else {
      String project = "ddt";
      String property = "ref0";
      List<Long> innerUids = new ArrayList<Long>();
      for (String mysqlHost : UidMappingUtil.getInstance().nodes()) {
        String filePath = "/Users/ytang1989/Workspace/testfiles/hadoop/60days_active_users/" + File.separator + project + "_" + mysqlHost;
        BufferedReader bufferedReader = null;
        System.out.println(filePath);
        try {
          bufferedReader = new BufferedReader(new FileReader(filePath));
          String tmpLine = null;
          while ((tmpLine = bufferedReader.readLine()) != null) {
            long innerUid = Long.parseLong(tmpLine) & 0xffffffffl;
            UserPropertyBitmaps.getInstance().markPropertyHit(project, innerUid, property);
            innerUids.add(innerUid);
          }
        } catch (FileNotFoundException e) {
          //do thing
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        } finally {
          if (bufferedReader != null)
            try {
              bufferedReader.close();
            } catch (IOException e) {
              LOG.error(e.getMessage(), e);
            }
        }
      }
      for (long innerUid : innerUids) {
        if (!UserPropertyBitmaps.getInstance().isPropertyHit(project, innerUid, property))
          System.out.println("error:" + innerUid);
      }



  }

  }
}



