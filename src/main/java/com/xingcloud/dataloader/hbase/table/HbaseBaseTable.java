package com.xingcloud.dataloader.hbase.table;

import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Author: qiujiawei
 * Date:   12-3-20
 */
public interface HbaseBaseTable {
    public HTableDescriptor getHTableDescriptor(String appid);
}
