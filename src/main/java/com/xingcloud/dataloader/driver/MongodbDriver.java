package com.xingcloud.dataloader.driver;


import com.mongodb.*;
import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.dataloader.hbase.table.EventMetaTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: qiujiawei
 * Date:   12-3-21
 */
public class MongodbDriver {
    public static final Log LOG = LogFactory.getLog(MongodbDriver.class);
    public static final String OBJ_ID = "_id";
    private Mongo mongo;
    private DB db;
    public DBCollection userColl;
    private DBCollection eventMetaColl;

    private static String taskFinishInfoColl = "dataloader_etl";


    public MongodbDriver() {
        try {


            mongo = new Mongo(StaticConfig.mongodbHost, StaticConfig.mongodbPort);
            db = mongo.getDB(StaticConfig.mongodbDBName);
            eventMetaColl = db.getCollection(StaticConfig.mongodbEventMetaCollectionName);

            eventMetaColl.ensureIndex(EventMetaTable.mongodbProjectIdField);
            for (String level : EventMetaTable.level) {
                eventMetaColl.ensureIndex(level);
            }

            LOG.info("connect to " + StaticConfig.mongodbHost + " " + StaticConfig.mongodbPort + " " + StaticConfig.mongodbDBName
                    + " " + StaticConfig.mongodbEventMetaCollectionName);

        } catch (Exception e) {
            LOG.error("mongodb init error", e);
        }
    }


    static private MongodbDriver instance;

    static public MongodbDriver getInstance() {
        if (instance == null) {
            instance = new MongodbDriver();
        }
        return instance;
    }


    static private DBCollection getEventMetaCollection() {
        try {
            return getInstance().eventMetaColl;
        } catch (Exception e) {
            LOG.error("get collection catch Exception", e);
        }
        return null;
    }

    static public long queryAllEventTotal = 0;
    static public long queryAllEventTimes = 0;

    public static DBCursor queryAllEvent(String project) {
        DBObject queryObject = new BasicDBObject();
        queryObject.put(EventMetaTable.mongodbProjectIdField, project);
        return getEventMetaCollection().find(queryObject);
    }

    private static AtomicLong pushEventTotal = new AtomicLong();
    private static AtomicLong pushEventTtimes = new AtomicLong();

    public static WriteResult pushEvent(String appid, String event, long time) {
        long start = System.currentTimeMillis();
        String[] temp = event.split("\\.");
        DBObject queryObject = new BasicDBObject();
        DBObject updateObject = new BasicDBObject();
        queryObject.put(EventMetaTable.mongodbProjectIdField, appid);
        updateObject.put(EventMetaTable.mongodbProjectIdField, appid);
        int i = 0;
        for (i = 0; i < temp.length; i++) {
            queryObject.put(EventMetaTable.level[i], temp[i]);
            updateObject.put(EventMetaTable.level[i], temp[i]);
            //LOG.info(temp[i]+" "+queryObject);
        }
        if (i < 6) {
            DBObject existObj = new BasicDBObject();
            existObj.put("$exists", false);
            queryObject.put(EventMetaTable.level[i], existObj);
        }
        // LOG.info(event+" "+queryObject);
        DBCursor cursor = getEventMetaCollection().find(queryObject);

        WriteResult re = null;
        if (cursor.size() == 0) {
            getEventMetaCollection().insert(updateObject);
        } else {
            DBObject update = new BasicDBObject();
            update.put("$set", updateObject);
            re = getEventMetaCollection().update(queryObject, update, true, true);

        }
        long end = System.currentTimeMillis();
        pushEventTotal.addAndGet(end - start);
        pushEventTtimes.incrementAndGet();
        return re;
    }

    public static WriteResult deleteEvent(String appid, String[] key, Object objID) {
        long start = System.currentTimeMillis();
        DBObject queryObject = new BasicDBObject();
//		DBObject updateObject=new BasicDBObject();
        queryObject.put(OBJ_ID, objID);
//		updateObject.put(EventMetaTable.STATUS, EventMeta.Status.deleted.ordinal());
        // LOG.info(event+" "+queryObject);
//		DBObject update=new BasicDBObject();
//		update.put("$set",updateObject);
        WriteResult re = getEventMetaCollection().remove(queryObject);
        //		update(queryObject, update,false,false);
        long end = System.currentTimeMillis();
        pushEventTotal.addAndGet(end - start);
        pushEventTtimes.incrementAndGet();
        return re;
    }

    public static WriteResult updateEvent(String project, String[] key, Object _id, long time) {
        long start = System.currentTimeMillis();
        DBObject queryObject = new BasicDBObject();
        DBObject updateObject = new BasicDBObject();
        queryObject.put(OBJ_ID, _id);
        updateObject.put(EventMetaTable.LAST_UPDATE, time);
        // LOG.info(event+" "+queryObject);
        DBObject update = new BasicDBObject();
        update.put("$set", updateObject);
        WriteResult re = getEventMetaCollection().update(queryObject, update, false, false);
        long end = System.currentTimeMillis();
        pushEventTotal.addAndGet(end - start);
        pushEventTtimes.incrementAndGet();
        return re;
    }


    public static void printAnalytics() {
        LOG.info("pushEventTotal is : " + pushEventTotal);
        LOG.info("pushEventTimes is : " + pushEventTtimes);
        LOG.info("distinctTotal is: " + queryAllEventTotal);
        LOG.info("distinctTimes is: " + queryAllEventTimes);
    }

    static public DBCursor getFinishStats(String date, String ip) {
        DBObject queryObject = new BasicDBObject();
        //todo: hostname似乎比ip更好？
        queryObject.put("inet_ip", ip);
        queryObject.put("date", date);
        queryObject.put("stats", "finish");
        return MongodbDriver.getInstanceDB().getCollection(taskFinishInfoColl).find(queryObject);
    }


    static public DB getInstanceDB() {
        if (instance == null) {
            instance = new MongodbDriver();
        }
        return instance.db;
    }

    static public Mongo getInstanceMongo() {
        if (instance == null) {
            instance = new MongodbDriver();
        }
        return instance.mongo;
    }

    public static void main(String[] args) {
        String event1 = "pay.pv.a";
        String event2 = "pay.pv";

        MongodbDriver.pushEvent("v9", event1, 111111111);
        MongodbDriver.pushEvent("v9", event2, 222222222);

        String event3 = "pay.pv.b";
        MongodbDriver.pushEvent("v9", event3, 333333333);
    }
}

















