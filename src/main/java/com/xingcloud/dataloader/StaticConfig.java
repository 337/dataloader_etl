package com.xingcloud.dataloader;

import com.xingcloud.util.config.ConfigReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.util.ArrayList;
import java.util.List;

/**
 * 初始化所有需要的配置文件信息
 * Author: qiujiawei
 * Date:   12-5-28
 */
public class StaticConfig {
    public static final Log LOG = LogFactory.getLog(StaticConfig.class);

    @Deprecated
    public static List<String> userProxySpecialProject = new ArrayList<String>() {{
        add("tencent-18894");
        add("rome0");
        add("zoom");

        add("age");
        add("tencent-16488");
        add("fishman");

        add("v9-mdg");
        add("ik2");
        add("happyranch");
        add("v9-sof");
        add("tencent-34650");

        add("v9-v9");

        add("xa-demo");
    }};


    static public int eventMaxTotal = 50000;
    static public int levelMax = 1000;
    /**
     * 这个列表里面的项目，其log不受到六层log每层的数量上限（1000）的限制。
     */
    static public List<String> eventMetaSpecialProjects = new ArrayList<String>() {{
        add("cloud-cloud");
        add("hesenstest");
        add("xc-rsc");
    }};


    static public boolean deuTableWalSwitch = false;
    static public boolean userProxyWalSwitch = false;

    static public String hdfsRoot = null;
    static public Configuration hbaseConf = null;

    static {
        try {
            hbaseConf = HBaseConfiguration.create();
            Configuration conf = new Configuration();

            hdfsRoot = conf.get("fs.default.name");
//            hdfsRoot = "hdfs://namenode.xingcloud.com:19000";
            System.out.println(hdfsRoot);

        } catch (Exception e) {
            LOG.error("init the hbase conf catch Exception", e);
        }
    }

    static public boolean compress = true;

    static public int readerPoolThreadNumber = 10;
    static public int submitPoolThreadNumber = 10;

    static {
        try {
            readerPoolThreadNumber = Integer.valueOf(ConfigReader.getConfig("dataloader_conf.xml", "reader"));
            submitPoolThreadNumber = Integer.valueOf(ConfigReader.getConfig("dataloader_conf.xml", "submiter"));
        } catch (Exception e) {
            LOG.error("init the readerPoolThreadNumber and  submitPoolThreadNumber conf catch Exception", e);
        }
    }

    static public String mongodbHost = "mongodb.xingcloud.com";
    static public int mongodbPort = 27021;
    static public String mongodbDBName = "user_info";

    static public String mongodbEventMetaCollectionName = "events_list";

    static {
        try {
            String host = ConfigReader.getConfig("Config.xml", "mongodb", "host");
            int port = Integer.parseInt(ConfigReader.getConfig("Config.xml", "mongodb", "port"));
            String dbName = ConfigReader.getConfig("Config.xml", "mongodb", "dbname");
            String eventMetaCollectionName = ConfigReader.getConfig("Config.xml", "mongodb", "eventmeataname");

            if (host != null) mongodbHost = host;
            mongodbPort = port;
            if (dbName != null) mongodbDBName = dbName;
            if (eventMetaCollectionName != null) mongodbEventMetaCollectionName = eventMetaCollectionName;

        } catch (Exception e) {
            LOG.error("init the mongodb conf catch Exception", e);
        }
    }


    static public String mysqlHost = "mysql.xingcloud.com";
    static public String mysqlPort = "3306";
    static public String mysqlUser = "xingyun";
    static public String mysqlPassword = "xa";
    static public String mysqlDb = "user_info";

    static {
        try {
            String host = ConfigReader.getConfig("dataloader_conf.xml", "mysql", "host");
            String port = ConfigReader.getConfig("dataloader_conf.xml", "mysql", "port");
            String user = ConfigReader.getConfig("dataloader_conf.xml", "mysql", "user");
            String password = ConfigReader.getConfig("dataloader_conf.xml", "mysql",
                    "password");
            String db = ConfigReader.getConfig("dataloader_conf.xml", "mysql", "db");

            if (host != null) mysqlHost = host;
            if (port != null) mysqlPort = port;
            if (user != null) mysqlUser = user;
            if (password != null) mysqlPassword = password;
            if (db != null) mysqlDb = db;

        } catch (Exception e) {
            LOG.error("init the mysql proxy info failed", e);
        }
    }

    static public String messageHostList = "";

    static {
        try {
            String hostList = ConfigReader.getConfig("dataloader_conf.xml", "messagehost");
            if (hostList != null) messageHostList = hostList;

        } catch (Exception e) {
            LOG.error("init the message host info failed", e);
        }
    }


    static public String siteDataList = "/home/hadoop/site_data";
    static public String storeLogList = "/home/hadoop/store_log";

    static {
        try {
            String siteList = ConfigReader.getConfig("dataloader_conf.xml", "site_data");
            String storeList = ConfigReader.getConfig("dataloader_conf.xml", "store_log");

            if (siteList != null) siteDataList = siteList;
            if (storeList != null) storeLogList = storeList;
        } catch (Exception e) {
            LOG.error("init the siteDataList and storeLogList host info failed", e);
        }
    }

    static public String userBackUpDefaultDir = "/data/UserBackUp/{date}";
    static public int userDefaultThreadNumber = 10;

    static public int realeaseBz2DefaultThreadNumber = 10;
    static public int copyBz2ToHistoryDefaultThreadNumber = 10;

//    public static void main(String[] args){
//        System.out.println(StaticConfig.hdfsRoot);
//    }
}
