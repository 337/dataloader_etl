package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.hbase.table.user.UserPropertyBitmaps;
import com.xingcloud.util.Constants;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

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
    for (String mysqlHost : UidMappingUtil.getInstance().nodes()) {
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
    for (String mysqlHost : UidMappingUtil.getInstance().nodes()) {

      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS + File.separator + project + "_" + mysqlHost;

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


  public static void main(String[] args) {
    getInstance().rebuildSixtyDays("xlfc", "20130715", 1);
    int[] innerUids = new int[]{1972106, 1972106, 1505918, 1969439, 1971945};
    for (int innerUid : innerUids) {
      for (String ref : User.refFields) {
        if (!UserPropertyBitmaps.getInstance().isPropertyHit("xlfc", innerUid, ref))
          System.out.println(ref + " error for " + innerUid);
      }
    }
  }
}


