package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.StaticConfig;

/**
 * Author: qiujiawei
 * Date:   12-3-12
 */
public class LocalPath {
    static public String SITE_DATA_LIST = StaticConfig.siteDataList;
    static public String STORE_LOG_LIST = StaticConfig.storeLogList;

    static public final String SITE_DATA="site_data";
    static public final String STORE_LOG="store_log";

    static public String getPathWithPrefix(String prefix,String appid,String date,int index){
        return prefix+"/"+appid+"/"+date.substring(0,4)+"/"+date.substring(4,6)+"/ea_data_"+date.substring(6,8)+"/"+index+".log";
    }




}
