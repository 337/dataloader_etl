package com.xingcloud.dataloader.lib;

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
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * User: IvyTang
 * Date: 13-7-15
 * Time: PM3:47
 */
public class RefBitMapRebuild {

  private static final Log LOG = LogFactory.getLog(RefBitMapRebuild.class);

  private static final RefBitMapRebuild instance = new RefBitMapRebuild();

  private RefBitMapRebuild() {
    //do nothing
  }

  public static RefBitMapRebuild getInstance() {
    return instance;
  }

  public void rebuildSixtyDays(String project, String date, int index) {
    if (index == Constants.FIRST_TASK_NUM) {
      rebuildSixtyDaysActiveUsersFromMySQL(project, date);
    } else {
      if (UserPropertyBitmaps.getInstance().ifPropertyNull(project, User.refField))
        rebuildSixtyDaysActiveUsersFromLocalFile(project);
    }
  }

  private void rebuildSixtyDaysActiveUsersFromMySQL(String project, String date) {

    UserPropertyBitmaps.getInstance().resetPropertyMap(project, User.refField);
    // load 60 active users from mysql to localfile.
    String[] nodes = UidMappingUtil.getInstance().nodes().toArray(new String[UidMappingUtil.getInstance().nodes()
            .size()]);
    shuffle(nodes);
    for (String mysqlHost : nodes) {
      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS + File.separator + project + "_" + mysqlHost;
      BufferedReader dumpBufferedReader = null;
      try {
        Runtime rt = Runtime.getRuntime();

        String sqlCMD = "mysql -uxingyun -pOhth3cha --database fix_" + project + " -h" + mysqlHost + " -ss -e\"SELECT" +
                " uid FROM last_login_time where val>=" + TimeIndexV2.getSixtyDaysBefore(date) + "000000\" >  " +
                filePath;

        System.out.println(sqlCMD);

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
    rebuildSixtyDaysActiveUsersFromLocalFile(project);
  }

  private void rebuildSixtyDaysActiveUsersFromLocalFile(String project) {

    UserPropertyBitmaps.getInstance().resetPropertyMap(project, User.refField);

    for (String mysqlHost : UidMappingUtil.getInstance().nodes()) {

      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS + File.separator + project + "_" + mysqlHost;

      System.out.println(filePath);

      BufferedReader bufferedReader = null;
      try {
        bufferedReader = new BufferedReader(new FileReader(filePath));
        String tmpLine = null;
        while ((tmpLine = bufferedReader.readLine()) != null) {
          long innerUid = Long.parseLong(tmpLine) & 0xffffffffl;
          UserPropertyBitmaps.getInstance().markPropertyHit(project, innerUid, User.refField);
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


  public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
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
        System.out.println(pids);

      }
    }

  }


}



