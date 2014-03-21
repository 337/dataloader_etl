package com.xingcloud.dataloader.hbase.table;


import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.dataloader.driver.MongodbDriver;
import com.xingcloud.dataloader.lib.Event;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import sun.reflect.generics.reflectiveObjects.LazyReflectiveObjectGenerator;

import java.util.*;

/**
 * 控制项目的事件列表，并提供查询接口判断输入的事件名符不符合要求。
 * <p/>
 * 对事件名的要求：
 * 6层；
 * 每层的子事件名不超过100；
 * 总的事件名不超过50000.
 * <p/>
 * 另外，在达到限制的情况下，如果有很久不更新的事件，则删除老事件，给新事件让位。
 * <p/>
 * Author: qiujiawei
 * Date:   12-3-19
 */
public class EventMetaTable {
    //index
    public static final String mongodbProjectIdField = "project_id";
    public static final String[] level = new String[]{"l0", "l1", "l2", "l3", "l4", "l5"};
    public static final String STATUS = "status";
    public static final String LAST_UPDATE = "last_update";
    //	public static final long TOO_OLD = 1000l * 60;//60 secs todo immars
    public static final long TOO_OLD = 1000l * 3600 * 24 * 30;//30 days

    private static final int MAX_EXCEED_NUM = 100;

    public static final Log LOG = LogFactory.getLog(EventMetaTable.class);
    public static final Log ERROR_LOG = LogFactory.getLog("dlerror");
    public int eventTotal = 0;
    private String project;

    //store the new event
    private Set<EventMeta> newEventCache = new HashSet<EventMeta>();

    //newly appeared event, but cannot add to system due to event count limitation
    private Set<EventMeta> exceedEventCache = new HashSet<EventMeta>();

    //changed event
    private Set<EventMeta> changedEventCache = new HashSet<EventMeta>();

    //too old event, possibly evacuated for exceedEvents.
    private MultiKeyMap<EventMeta> oldEventCache = new MultiKeyHashMap<EventMeta>();

    //store all event map cache
    private MultiKeyMap<EventMeta> eventMap = new MultiKeyHashMap<EventMeta>();


    public EventMetaTable(String project) {
        this.project = project;
        init();

    }

    private void init() {
        try {

            long start = System.currentTimeMillis();
            eventTotal = 0;
            DBCursor cursor = MongodbDriver.queryAllEvent(project);
            while (cursor.hasNext()) {
                DBObject dbObject = cursor.next();
                if (dbObject.get(STATUS) != null && EventMeta.Status.values()[(Integer) dbObject.get(STATUS)] == EventMeta.Status.deleted) {
                    continue;
                }
                eventTotal++;
                List<String> keys = new ArrayList<String>();
                for (int i = 0; i < Event.eventFieldLength; i++) {
                    String val = (String) dbObject.get(EventMetaTable.level[i]);
                    if (val != null) {
                        keys.add(val);
                    } else {
                        break;
                    }
                }
                String[] keyArray = keys.toArray(new String[0]);
                Long val = (Long) dbObject.get(EventMetaTable.LAST_UPDATE);
                long lastUpdate = 0;
                EventMeta eventMeta = new EventMeta(keyArray, lastUpdate, dbObject.get(MongodbDriver.OBJ_ID));
                if (val == null) {
                    lastUpdate = System.currentTimeMillis();
                    dirty(eventMeta);
                } else {
                    lastUpdate = val;
                }
                eventMeta.lastUpatetime = lastUpdate;
                eventMap.put(eventMeta, keyArray);

                //prepare for old events
                if (eventMeta.lastUpatetime < start - TOO_OLD) {
                    oldEventCache.put(eventMeta, eventMeta.key);
                }
            }


            LOG.info(project + "has event :" + eventMap.size());
            LOG.info(project + "has old event :" + oldEventCache.size());
            long end = System.currentTimeMillis();
            MongodbDriver.queryAllEventTimes += (end - start);
            MongodbDriver.queryAllEventTotal++;
            LOG.info(project + "init mongo event using " + (end - start) + " ms.");
        } catch (Exception e) {
            LOG.error("mongo exception," + project, e);

        }
    }


    /**
     * 检查event合法性：每层事件数不超过上限（1000个）
     *
     * @param event
     * @return 合法的事件，返回true
     */
    public boolean checkEvent(Event event) {
//        LOG.info("check event");
        String[] key = normalize(event.getEventArray());
        if(key.length == 0)
          return false;
        EventMeta eventMeta = eventMap.get(key);
        if (eventMeta != null) {
            dirty(eventMeta);
            return true;
        } else if (eventTotal >= StaticConfig.eventMaxTotal) {
            exceed(new EventMeta(key, 0, null), EventMeta.Status.exceedingTotal, 0);
            ERROR_LOG.error("The event " + key +" is ignored as the total event number(" + eventTotal + ") >= max event(" +  StaticConfig.eventMaxTotal + ")");
            return false;
        } else {
            for (int i = 0; i < Event.eventFieldLength && i < key.length; i++) {
                Set<String> dir = eventMap.dir(Arrays.copyOf(key, i));
                if (dir.contains(key[i])) {
                    continue;
                } else {
                    if (dir.size() >= StaticConfig.levelMax) {
                        exceed(new EventMeta(key, 0, null), EventMeta.Status.exceedingLevel, i);
                        return false;
                    }
                }
            }
            eventTotal++;
            EventMeta meta = new EventMeta(key, 0, null);
            eventMap.put(meta, meta.key);
            newEventCache.add(meta);
            return true;
        }
    }

    private void exceed(EventMeta eventMeta, EventMeta.Status status, int i) {
        if (exceedEventCache.size() >= MAX_EXCEED_NUM) {
            return;
        }
        eventMeta.status = status;
        eventMeta.level = i;
        exceedEventCache.add(eventMeta);
    }

    private void dirty(EventMeta eventMeta) {
        if (!newEventCache.contains(eventMeta)) {
            changedEventCache.add(eventMeta);
        }
    }


    private String[] normalize(String[] eventArray) {
        if (eventArray == null) {
            return new String[0];
        }
        int trueLength = eventArray.length;

        for (int i = 0; i < eventArray.length; i++) {
            if (eventArray[i] == null || "".equals(eventArray[i])) {
                trueLength = i;
                break;
            }
        }
        if (trueLength != eventArray.length) {
            return Arrays.copyOf(eventArray, trueLength);
        } else {
            return eventArray;
        }
    }

    public void debugprint() {
//        LOG.info("begin********************************");
//        for(Map.Entry<LevelKey,List<String>> kv:eventMap.entrySet()){
//            LOG.info(kv.getKey()+" "+kv.getValue());
//        }
//        LOG.info("end********************************");
    }

    //********************************************************************

    //********************************************************************

    /**
     * @return
     */
    public int flushMongodb() {
        long startTime = System.nanoTime();
        LOG.info(project + " flushing event meta table. creating new events...");
        long now = System.currentTimeMillis();
        //push newly added events
        for (EventMeta meta : newEventCache) {
            MongodbDriver.pushEvent(project, StringUtils.join(meta.key, "."), now);
        }

        LOG.info(project + " try deleting old events...");
        //delete old events if necessary
        LOG.info(project + " exceedEventCache size:" + exceedEventCache.size());
        for (EventMeta meta : exceedEventCache) {

            if (oldEventCache.size() == 0) {
                break;
            }
//            LOG.info(project + " exceedEventCache " + Arrays.toString(meta.key));
//			EventMeta oldMeta;
            switch (meta.status) {
                case exceedingTotal:
                    for (EventMeta oldMeta : oldEventCache) {
                        MongodbDriver.deleteEvent(project, oldMeta.key, oldMeta._id);
                        oldEventCache.remove(oldMeta.key);
                        //LOG.info(project + "delete " + Arrays.toString(oldMeta.key) + " for exceedingTotal");
                        break;
                    }
                    break;
                case exceedingLevel:
                    if (meta.key.length == 0) {
                        break;
                    }
                    String[] exceedingKey = Arrays.copyOf(meta.key, meta.level + 1);
                    if (exceedingKey.length == 0) {
                        break;
                    }
                    String[] parentKey = Arrays.copyOf(exceedingKey, exceedingKey.length - 1);
                    Set<String> dir = oldEventCache.dir(parentKey);
                    if (dir.size() != 0) {
                        Iterator<EventMeta> it = oldEventCache.iterator(parentKey);
                        if (it.hasNext()) {
                            EventMeta oldMeta = it.next();
                            MongodbDriver.deleteEvent(project, oldMeta.key, oldMeta._id);
                            oldEventCache.remove(oldMeta.key);
                            LOG.debug("delete old[" + Arrays.toString(oldMeta.key) + "] for new[" + Arrays.toString(meta
                                    .key) + "],");
                            LOG.debug("because " + Arrays.toString(exceedingKey) + " cannot insert into " + Arrays
                                    .toString(parentKey) + "");
                            dir.size();
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        LOG.info("updating old events...");
        //update last update time of events
        for (EventMeta meta : changedEventCache) {
            MongodbDriver.updateEvent(project, meta.key, meta._id, now);
        }
        LOG.info(project + " Flush mongodb taken " + (System.nanoTime() - startTime) / 1.0e9 + " sec");
        return newEventCache.size();
    }

    public int eventNumber() {
        return newEventCache.size() + exceedEventCache.size();
    }


    public static void main(String[] args) {
//        test();
    }

    private static void test() {
        EventMetaTable emt = new EventMetaTable("test_storm");
        Random rand = new Random();
        int range = 15;
        for (int i = 0; i < 100; i++) {
            String[] key = {"test", String.valueOf(rand.nextInt(range)), String.valueOf(rand.nextInt(range)), String.valueOf(rand.nextInt(range))};
            emt.checkEvent(new Event("321443", key, 0, System.currentTimeMillis()));
        }
        emt.flushMongodb();
        System.out.println("rand = " + rand);
    }
}
