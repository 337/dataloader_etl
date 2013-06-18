package com.xingcloud.dataloader.hbase.tableput;


import com.xingcloud.dataloader.etloutput.DelayUserOutput;
import com.xingcloud.dataloader.etloutput.UserOutput;
import com.xingcloud.dataloader.hbase.table.user.UserProxy;
import com.xingcloud.dataloader.lib.Event;
import com.xingcloud.dataloader.lib.User;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 只更新mysql的tableput
 * Author: qiujiawei
 * Date:   12-6-12
 */
public class UserTablePut implements TablePut {
    public static final Log LOG = LogFactory.getLog(CombineTablePut.class);
    private UserProxy userProxy;
    private int taskNumber = 10;

    private String project;
    private String date;
    private int index;

    public UserTablePut(String project, String date, int index) {
        this.project = project;
        this.date = date;
        this.index = index;
        userProxy = new UserProxy(project, date, index);
    }


    public synchronized boolean put(Event event) {
        try {
            userProxy.addEvent(event);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return false;
        }
        return true;
    }

    @Override
    public synchronized void flushToLocal() throws IOException {
        List<User> users = new ArrayList<User>();
        users.addAll(userProxy.getPutUsers().values());

        List<User> delayUsers = new ArrayList<User>();
        delayUsers.addAll(userProxy.getDelayUsers().values());

        UserOutput.getInstance().write(project, users);
        DelayUserOutput.getInstance().write(project, delayUsers);
    }
}
