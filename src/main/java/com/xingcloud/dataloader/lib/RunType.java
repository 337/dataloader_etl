package com.xingcloud.dataloader.lib;

/**
 *  * 类型有3种分别是
 * 只更新hbase RunType.EventOnly
 * 只更新mysql RunType.UserOnly
 * 同时更新hbase和mysql RunType.Normal
 *
 * Author: qiujiawei
 * Date:   12-6-12
 */
public enum RunType {
    Normal,UserOnly,EventOnly
}
