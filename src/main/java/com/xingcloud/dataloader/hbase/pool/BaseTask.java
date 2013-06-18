package com.xingcloud.dataloader.hbase.pool;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.SimpleDateFormat;

/**
 * Author: qiujiawei
 * Date:   12-3-21
 */
public abstract class BaseTask implements Runnable {
    public static final Log LOG = LogFactory.getLog(BaseTask.class);
    protected static final SimpleDateFormat df = new SimpleDateFormat("hh:mm:ss");
    private int taskNumber;


    public void setTaskNumber(int taskNumber) {
        this.taskNumber = taskNumber;
    }

    public int getTaskNumber() {
        return taskNumber;
    }

    @Override
    public abstract String toString();

}
