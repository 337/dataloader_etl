package com.xingcloud.dataloader.server;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.xingcloud.dataloader.driver.MongodbDriver;
import com.xingcloud.dataloader.lib.TimeIndexV2;
import com.xingcloud.util.manager.NetManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * User: IvyTang
 * Date: 13-3-4
 * Time: 下午1:57
 */
public class ETLScheduleMonitor implements Runnable {

  private static final Log LOG = LogFactory.getLog(ETLScheduleMonitor.class);

  private static final int CHECK_INTERVAL_MS = 1 * 60 * 1000;

  private static final long INITIATE_SLEEP_TIME = 2 * 3600 * 1000l;

  public static boolean ifStillRunn = true;


  @Override
  public void run() {
    LOG.info("begin run ETLScheduleMonitor.");
    try {
      Thread.sleep(INITIATE_SLEEP_TIME);
    } catch (InterruptedException e) {
      LOG.error("InterruptedException", e);
    }
    while (ifStillRunn) {
      try {
        checkLastTask();
        Thread.sleep(CHECK_INTERVAL_MS);
      } catch (InterruptedException e) {
        LOG.error("InterruptedException", e);
      }

    }
    LOG.info("ETLScheduleMonitor exit.");
  }

  private void checkLastTask() {
    long currentTS = System.currentTimeMillis();
    int lastTaskIndex = TimeIndexV2.getLastTimeIndex(currentTS);
    int lastTaskDay = TimeIndexV2.getLastTaskYearMonthDay(currentTS);

    DBObject queryObject = new BasicDBObject();
    queryObject.put("inet_ip", NetManager.getSiteLocalIp());
    queryObject.put("date", String.valueOf(lastTaskDay));
    queryObject.put("index", lastTaskIndex);
    queryObject.put("stats", "finish");
    DBCursor dbCursor = null;
    try {
      dbCursor = MongodbDriver.getInstanceDB().getCollection("dataloader_etl").find(queryObject);

      if (!dbCursor.hasNext()) {
        LOG.error(lastTaskDay + "\t" + lastTaskIndex + " hasn't finished in 5 mins.");
      } else {
        DBObject temp = dbCursor.next();
        LOG.info(lastTaskDay + "\t" + lastTaskIndex + " has finished.using " + temp.get("time") + "ms.");
      }
    } catch (Exception e) {
      LOG.error("check monitor error.", e);
    } finally {
      if (dbCursor != null)
        dbCursor.close();
    }
  }


}


