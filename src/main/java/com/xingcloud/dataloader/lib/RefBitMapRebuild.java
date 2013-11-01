package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.hbase.table.user.UserPropertyBitmaps;
import com.xingcloud.util.Constants;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
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

    LOG.info("rebuildSixtyDays for project: " + project);

    if (index == Constants.FIRST_TASK_NUM) {
      rebuildSixtyDaysActiveUsersFromMySQL(project, date);
    } else {
      if (UserPropertyBitmaps.getInstance().ifPropertyNull(project, User.refField)) {
        rebuildSixtyDaysActiveUsersFromLocalFileV2(project, canRecoverFromLocalFiles);
      }
    }
  }

  private void rebuildSixtyDaysActiveUsersFromMySQL(String project, String date) {
    dumpSixtyDaysActiveUsersToLocal(project, date);
    rebuildSixtyDaysActiveUsersFromLocalFileV2(project, new String[]{User.refField});
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


  private void rebuildSixtyDaysActiveUsersFromLocalFileV2(String project, String[] properties) {
    for (String property : properties) {
      UserPropertyBitmaps.getInstance().resetPropertyMap(project, property);
      UserPropertyBitmaps.getInstance().initPropertyMap(project, property);
    }

    long beginning = System.currentTimeMillis();
    long maxUid = Long.MIN_VALUE, minUid = Long.MAX_VALUE;

    //first, determine the max uid and min uid
    for (String mysqlHost : UidMappingUtil.getInstance().nodes()) {
      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS + File.separator + project + "_" + mysqlHost;
      LOG.info(project + " read " + filePath);

      BufferedReader bufferedReader = null;
      try {
        bufferedReader = new BufferedReader(new FileReader(filePath));
        String tmpLine = null;
        while ((tmpLine = bufferedReader.readLine()) != null) {
          long innerUid = Long.parseLong(tmpLine) & 0xffffffffl;
          if (innerUid > maxUid) {
            maxUid = innerUid;
          }
          if (innerUid < minUid) {
            minUid = innerUid;
          }
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      } finally {
        if (bufferedReader != null) {
          try {
            bufferedReader.close();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }
    }

    LOG.info("max uid: " + maxUid + ", min uid: " + minUid);

    //next, initialize bitmap
    if (maxUid != Long.MIN_VALUE && minUid != Long.MAX_VALUE) {
      for (String property : properties) {
        UserPropertyBitmaps.getInstance().markPropertyHit(project, minUid, property);
        UserPropertyBitmaps.getInstance().markPropertyHit(project, maxUid, property);
      }
    }

    //the last...
    for (String mysqlHost : UidMappingUtil.getInstance().nodes()) {
      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS + File.separator + project + "_" + mysqlHost;
      LOG.info(project + " read " + filePath);

      BufferedReader bufferedReader = null;
      try {
        bufferedReader = new BufferedReader(new FileReader(filePath));
        String tmpLine = null;
        while ((tmpLine = bufferedReader.readLine()) != null) {
          long innerUid = Long.parseLong(tmpLine) & 0xffffffffl;
          for (String property : properties) {
            UserPropertyBitmaps.getInstance().markPropertyHit(project, innerUid, property);
          }
        }
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      } finally {
        if (bufferedReader != null) {
          try {
            bufferedReader.close();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
        }
      }
    }

    System.out.println(project + " rebuild ref,nation,register_time,geoip from local file using " +
      (System.currentTimeMillis() - beginning) + "ms.");
  }


  public static void main(String... args) {
    RefBitMapRebuild refBitMapRebuild = new RefBitMapRebuild();
    refBitMapRebuild.rebuildSixtyDaysActiveUsersFromLocalFileV2("ddt", refBitMapRebuild.canRecoverFromLocalFiles);
  }
}
