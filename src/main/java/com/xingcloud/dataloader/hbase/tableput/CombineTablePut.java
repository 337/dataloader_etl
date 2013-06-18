package com.xingcloud.dataloader.hbase.tableput;

import com.xingcloud.dataloader.lib.Event;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * 同时更新hbase和mysql的tableput
 * <p/>
 * Author: qiujiawei
 * Date:   12-3-19
 */
public class CombineTablePut implements TablePut {

    public static final Log LOG = LogFactory.getLog(CombineTablePut.class);

    private EventTablePut eventTablePut;

    private UserTablePut userTablePut;


    private String project;
    private String date;
    private int index;


    public CombineTablePut(String project, String date, int index) {
        this.project = project;
        this.date = date;
        this.index = index;

        this.eventTablePut = new EventTablePut(project, date, index);
        this.userTablePut = new UserTablePut(project, date, index);
    }

    public synchronized boolean put(Event event) {
        //检查事件是否合法，不能超多事件类型限制
        if (eventTablePut.put(event)) {
            userTablePut.put(event);
            return true;
        }
        return false;
    }

    @Override
    public synchronized void flushToLocal() throws IOException {
        userTablePut.flushToLocal();
        eventTablePut.flushToLocal();
        eventTablePut.flushEventToMongo();


    }
}
