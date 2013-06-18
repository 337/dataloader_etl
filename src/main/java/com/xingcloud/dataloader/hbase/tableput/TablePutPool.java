package com.xingcloud.dataloader.hbase.tableput;

import com.xingcloud.dataloader.BuildTableAdmin;
import com.xingcloud.dataloader.lib.RunType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 保存所有项目的tableput，并根据类型，实例化对应tableput对象
 * Author: qiujiawei
 * Date:   12-5-17
 */
public class TablePutPool {
    public static final Log LOG = LogFactory.getLog(TablePutPool.class);

    private Map<String, TablePut> tablePutMap = new HashMap<String, TablePut>();
    private RunType runType;

    public TablePutPool(RunType runType) {
        this.runType = runType;
    }

    public TablePut getTablePut(String project, String date, int index) throws Exception {
        TablePut tablePut = tablePutMap.get(project);
        if (tablePut == null) {
            BuildTableAdmin.ensureProject(project);
            if (runType == RunType.Normal) {
                tablePut = new CombineTablePut(project, date, index);
            } else if (runType == RunType.EventOnly) {
                tablePut = new EventTablePut(project, date, index);
            } else if (runType == RunType.UserOnly) {
                tablePut = new UserTablePut(project, date, index);
            }
            tablePutMap.put(project, tablePut);
        }
        return tablePut;
    }
}
