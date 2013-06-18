package com.xingcloud.dataloader.hbase.tableput;

import com.xingcloud.dataloader.etloutput.DelayOutput;
import com.xingcloud.dataloader.etloutput.EventOutput;
import com.xingcloud.dataloader.hbase.table.DeuTable;
import com.xingcloud.dataloader.hbase.table.EventMetaTable;
import com.xingcloud.dataloader.lib.Event;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 只更新hbase的tableput
 * Author: qiujiawei
 * Date:   12-6-12
 */
public class EventTablePut implements TablePut {
    public static final Log LOG = LogFactory.getLog(EventTablePut.class);

    //事件meta表控制
    private EventMetaTable eventMetaTable;

    //deu表更新对象
    private DeuTable deuTable;

    private long eventCount = 0;

    //默认批量put的任务分割数量
    private int taskNumber = 10;

    private String project;
    private String date;
    private int index;


    public EventTablePut(String project, String date, int index) {
        this.project = project;
        this.date = date;
        this.index = index;

        eventMetaTable = new EventMetaTable(project);
        deuTable = new DeuTable(project, date);
    }


    public synchronized boolean put(Event event) {
        if (eventMetaTable.checkEvent(event)) {
            deuTable.addEvent(event);
            eventCount++;
            return true;
        } else return false;
    }

    @Override
    public synchronized void flushToLocal() throws IOException {
        List<Event> allEvents = new ArrayList<Event>();
        for (List<Event> tmpEvents : deuTable.getPutEvents().values())
            allEvents.addAll(tmpEvents);

        List<Event> delayEvents = new ArrayList<Event>();
        for (List<Event> tmpEvents : deuTable.getDelayEvents().values())
            delayEvents.addAll(tmpEvents);

        EventOutput.getInstance().write(project, allEvents);
        DelayOutput.getInstance().write(project, delayEvents);
    }

    public synchronized void flushEventToMongo() {
        eventMetaTable.flushMongodb();
    }


}
