package com.xingcloud.dataloader.hbase.tableput;

import com.xingcloud.dataloader.hbase.pool.BaseTask;
import com.xingcloud.dataloader.lib.Event;

import java.io.IOException;
import java.util.List;

/**
 * Author: qiujiawei
 * Date:   12-3-19
 */
public interface TablePut {
    /**
     * 接受事件处理
     *
     * @param event 要处理的事件
     * @return 是否能成功处理
     */
    public boolean put(Event event);


    public void flushToLocal() throws IOException;
}
