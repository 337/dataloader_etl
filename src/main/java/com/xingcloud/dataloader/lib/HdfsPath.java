package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.StaticConfig;

/**
 * Author: qiujiawei Date: 12-3-22
 */
public class HdfsPath {

    static public String hdfsRoot = StaticConfig.hdfsRoot;
    // static public final String
    // hdfsRoot="hdfs://namenode.xingcloud.com:19000/";
    static public String HdfsPath = StaticConfig.hdfsRoot + "/user/hadoop/analytics";

    static public String getPath(String type, String appid) {
        // return
        // getTypePath(type)+project+"/"+date.substring(0,4)+"/"+date.substring(4,6)+"/ea_data_"+date.substring(6,8)+"/"+index+".log";
        if (type.equals(USER_COLL))
            return HdfsPath + "/" + appid + "/user/coll/";
        else
            return null;
    }

    static final public String USER_COLL = "user_coll";

    public enum DIRTYPE{
        USER_COLL,ANALYTICS,ROOT,SITE_DATA,STORE_LOG,DIY;
    }
    static public String getHdfsDir(DIRTYPE dirtype) throws Exception {
        return getHdfsDir(dirtype,null,null);
    }
    static public String getHdfsDir(DIRTYPE dirtype,String appid,String date) throws Exception {
        switch (dirtype){
            case USER_COLL: return HdfsPath + "/" + appid + "/user/coll/";
            case ANALYTICS: return HdfsPath;
            case ROOT: return hdfsRoot;
            case SITE_DATA:
            case STORE_LOG:
            case DIY:
                return getHdfsDateDir(dirtype,appid,date);

        }
        return null;
    }
    static private String getHdfsDateDir (DIRTYPE dirtype,String appid,String date) throws Exception{
        if(date.length()!=8) throw new Exception("date is wrong");
        String year=date.substring(0, 4);
        String month=date.substring(4,6);
        String day=date.substring(6,8);
        switch (dirtype){
            case SITE_DATA:  return HdfsPath+"/" + appid + "/" + year + "/" + month + "/ea_data_" + day + "/data/";
            case STORE_LOG:  return HdfsPath+"/" + appid + "/" + year + "/" + month + "/ea_data_" + day + "/store/";
            case DIY:   return HdfsPath+"/" + appid + "/" + year + "/" + month + "/ea_data_" + day + "/diy/";
        }
        return null;
    }
}
