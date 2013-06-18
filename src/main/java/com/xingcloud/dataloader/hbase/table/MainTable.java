package com.xingcloud.dataloader.hbase.table;

import com.xingcloud.dataloader.lib.Event;
import org.apache.hadoop.hbase.client.Put;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Author: qiujiawei
 * Date:   12-3-19
 */
public interface MainTable extends HbaseBaseTable {
    public String getTableName(String projectIdentifier);
    public void addEvent(Event event);
    public Map<byte[], List<Event>> getPutEvents();
}
