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

    public static final int V4_ACTION_ITEMS_NUM = 5;

    public static final int V4_UPDATE_ITEMS_NUM = 6;

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


    static public int eventMaxTotal = 5000000;
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

    static public int readerPoolThreadNumber = 12;
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
//            String host = ConfigReader.getConfig("Config.xml", "mongodb", "host");
            String host = "65.255.35.144";
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
    static public String v4LogList = "/home/hadoop/v4_log";
    static {

        try {
            String siteList = ConfigReader.getConfig("dataloader_conf.xml", "site_data");
            String storeList = ConfigReader.getConfig("dataloader_conf.xml", "store_log");
            String v4List = ConfigReader.getConfig("dataloader_conf.xml", "v4_log");

            if (siteList != null) siteDataList = siteList;
            if (storeList != null) storeLogList = storeList;
            if (v4List != null) v4LogList = v4List;
        } catch (Exception e) {
            LOG.error("init the siteDataList and storeLogList and v4LogList host info failed", e);
        }
    }

    static public String userBackUpDefaultDir = "/data/UserBackUp/{date}";
    static public int userDefaultThreadNumber = 10;

    static public int realeaseBz2DefaultThreadNumber = 10;
    static public int copyBz2ToHistoryDefaultThreadNumber = 10;

//    public static void main(String[] args){
//        System.out.println(StaticConfig.hdfsRoot);
//    }
    static public String[] invalidpids = new String[]{"newtabv3-bg","newtabv1-bg","extended-protection","focalpricetest1","sof-hpprotect",
        "v9-sof","v9-mdd-zs","sof-gl","qone8search","sof-hpnt","citylife","fishman","v9-fft-2","v9-fft",
        "picatowntest","zoom","qone8-search","lollygame","sa2-single","sof-app-wallpaper","sof-pc","v9-fft-1",
        "v9-bnd","v9-ins","defender","goe","v9-gzg","v9-iob","v9-gdp","snsnations","v9-vei","v9-slb","wushen-ko",
        "foreststory-local","v9-idg","reputations","sof-dealply","twpoker","tw","v9-ss","v9-mlv","tencent-100619972",
        "sof-asc","sof-fxt","sof-mb","v9-mdd-jl","v9-referral","v9-vlt","appstudo","ep-404pages","justfortest","v9-wnf",
        "safev9","ramweb","pls","m-appstudo","search-switch","softmgr","bomcelular","lea","sof-youtv","omiga-search",
        "v9-gp","leawo","delta","tj","v9-ind","mobilewallpaper","v9-avc-1","v9-avc","v9-v9tb","searchwidget","v9-ism",
        "dinokingdom","v9-smk","v9-net","v9-umz-2","v9-nps","priceangels2","elexmarket","v9-v9speed","v9-mdd-lh","v9-trnd",
        "newtab-iframe","bookmark-v9","msdk-aoe","hdzdy","xiaoxiaoyedian","zhuguan","v9-iob-1","v9-gls","pirateking","mhc",
        "tencent-16488","zzzw","crazytribe","boyapoker","rxzq","v9-dvi","efunfunsm4","coalaatexaspoker","yhzg","v9-klt",
        "ponyland","ks","v9-muh","v9-ssrks","v9-imb","v9-jet","pmxy","v9-idd","v9-v9sc","v9-atr","civwar-integrate","rs",
        "mohuanhuayuan","magic-gardener","hl","v9-bdl","longjiang","v9-wbp","v9-ascbxk","v9-js","v9-imm","haidao",
        "pirates-defender","sydhold","v9-rek","v9-afp","v9-kw","v9-sssmz","v9-lgn","v9-kin","limingdiguo","v9-mdg",
        "v9-umz","chuanshuo","casino","card","qlj","v9-sfp","cafelife","ddtschool","dzpk","nindou","gotgp","mdd-search-widget",
        "v9-vlt2","v9-slbnew","apptoolsserver","gs","v9-see","v9-stk","v9-hanp","farmerama","yitien","apptools-appstudo","darkorbit",
        "sof-plushd","v9-dsk","v9-edep","tencent-30209","yulong","v9-nsk","v9-mdd-sk","nizhuantianxiakrad","sof-costmin","v9-birp",
        "v9-sstbf","v9-kb","v9-amt","v9-kts","v9-lwaviguanwang","xiyouqzhuan","v9-fog","v9-dnk","v9-v9sm","sof-yy","feiquanqiu",
        "v9-tti","soft-alo","sof-50onred","tradetang2","happytreasure","v9-gtl","tencent-30050","ofm","v9-durp","v9-fxtbxk","sftx",
        "sof-getsavin","ddtschoolorkut","sof-gc","romemanor","v9-vip","fragorial1","v9-smt","sof-cut","mhdxd","v9-adk","v9-rks",
        "v9-vkfp","ecyber-ph","sof-fsab","nizhuantianxiakr","battledawn","v9-mdd-gtl","dtzl2","rummikub","v9-test2","v9-ssfds","sa2-online",
        "v9-mrm","v9-veo","sof-ls","doom","kaku","v9-vdl","sinohotel","onmylike-mdg","lc","v9-ishp","v9-adc","v9-lgb","rifthunter",
        "footballidentity","v9-ish","v9-umz-1","find-sof","v9-ktl","v9-rdlgb","v9-asca5","v9-dsk4sh","v9-atv","zx","cloudunion","r","v9-utt",
        "v9-ssybc","moshi","v9-adwordsgoplayer","v9-hitb","sof-kp","yiqifei","sof-yys","v9-indprt","camelgames","mw2","sa2-test","v9-vsb","v9-mdd-aoe",
        "v9-rdops","sof-fil","yyptsite","ecyber-lanucherwnd","chaos-age","v9-8th","v9-ssrjc","tencent-100630156","v9-abc","v9-mdd-searchapk",
        "fb-get-test","dhgate1","v9-w3i","jdbbx","v9-hipp","v9-vtt","v9-ascadt2","v9-ascadt3","v9-hipb","v9-mdd-91","baby","sof-msn","v9-idg2new",
        "tencent-100623395","onmylike-jdl","msnshell3","dinodirect","v9-rjc","tencent-19089","v9-ssnet","lumosity-demo","xtreeme","skg","focalprice",
        "cuteplant","foxit1","v9-asdguanwang","v9-mcguanwang","sof-mel","fish-hero","lj","qudongrensheng","v9-fxtzig","v9-asc","v9-asq","tag-zhou",
        "v9-vltnew","hop2","livemall","v9-asclgn","shangwang","fish","mdd","milan","v9-tek","v9-retp","v9-epo","kingnetshushan","test0005","manortest",
        "tidebuy","v9-mti","cufflinks","v9-ssf46","sof-js","longjiang-dajiangjun","madeinchina","v9-ascutd","ewin88","v9-ascspg","v9-avs","sof-ckt",
        "minilyrics","xc-rsc","elongdemo","tencent-100635540","v9-ascc02","v9-rdrd","eachbuyer","v9-test1","xlfc-3","xlfc-4","v9-mizp","v9-ssmrm",
        "sof-llq","longjiangnn","v9-melzig","v9-melguanwang","v9-vcs","tradetang","v9-ascbxe","v9-kpguanwang","sof-hpp-asc","v9-sinp","test-common-ml",
        "v9-sszig","v9-utd","yoybuy1","yellowearth","sof-itl","xlfc-2","v9-mel","v9-test3","ppt-assistant","v9-ascstp","v9-ascvlt","v9-idgnew","ddtest",
        "sof-hpp","v9-ssrek","yourlydia","ems360","v9-sssmk","v9-ssfdl","v9-ade","v9-sssmt","v9-ssb10","testing","tencent-33983","v9-ssbxe",
        "qiudechao-site","pirate","v9-ascsmk","jojo207","v9-ssa1","v9-imi","v9-vizp","v9-aschai","ecyber-tr","v9-ismc","db-monitor","smzt","v9-ssedl",
        "studentuniverse","esupin","asdf","bowenshangcheng","v9-ilk","v9-mdd-webpage","v9-bsh","v9-mrl","tupianzhuanhuan","v9-ssfid","qqv3doc",
        "find-find","cetetek","ec21","v9-sshfd","v9-ascc10","v9-ssgls","sof-mc","v9-ascpzg","v9-ioy","v9-maip","efunfuntest","v9-dskguanwang","fluege",
        "v9-kdl","v9-asczig","yy-helpcenter","tencent-18271","everytide","v9-adwordsnurse","nowec","tencent-100624412","smartreversi","dreammail",
        "tencent-34650","elexv9nurse","v9-ssb7","v9-ssb6","v9-ssb5","test2","v9-ssb3","v9-ssb1","easyhadoop","v9-ssb9","v9-ssb8","ctrip-demo",
        "v9-mdd-ajzs","v9-fxtguanwang","bnb2b","cloud-cloud","v9-ascnur","v9-ssb4","ucenter","mltest1","pokerstarswebsite","jike","dealextreme",
        "v9-asc4sh","dssynctest","v9-4sh","v9-sskts","v9-rdguanwang","v9-ascidd","software","tencent-10152","longjiangn","yoygou1","swjy","v9-sdg",
        "sof-asd","v9-fxta1","v9-fxta3","v9-fxta5","landing","dresses1","v9-ibr","v9-ssadn","v9-ascedl","jcsportline","onmylike-onmylike","ecyber",
        "mokredit","en-ec21","qqdownload","v9-dtn","v9-ssctv","v9-sshai","markettime","mltest","ku123","v9-fxtxzz","v9-ascnpg","susinobag","v9-ascnps",
        "v9-mdd","piratestorm","v9-search","v9-edeb","sof-ftchrome","savetome","sof-pcsm","s337","xiaomi","gt","mxhzw","sof-yontoo","hot-finder","mozca",
        "sof-iminent","fishao-de","candymahjong","letsfarm-dev","kkpoke21","yahoo","v9-ttinew","search-savetome","tw1","sof-yandex","dragonkokaoadreally",
        "arcadecenter","battlealertatapple","letsfarm","northeuropekr","v9-ssvyk","longzhizhaohuankoad","soft-twitter-assist","sof-jwallet","v9-mhc",
        "uf-pay","sof-ftchrome1","yes-test","solitaireduels","sof-lksav","raydownload","v9-vtnet","sof-iapps","hdtexaspoker","happ_1ch","nztx",
        "kd","sof-addlyrics","farm3_meinvz","iobit","fiwhman","sof-pcsuzk","s","v9-mtix","v9-rdm","yac-gdpdl","pt","sof-lol","iappyfarmer",
        "boyojoy","tcg","tibia","default-newtab","mhsj","kongregatetest","xing","mahjongduels","sof-pcf","hk","krlong","yac-updl","pgzs","zhongqinglv",
        "atest","ns","drakensang","menghuan","sof","sof-bp","xlfc-cbnc","monster","kongfu","xlfcmobile","longzhizhaohuan","minigarden","sz-eng","v9-tug",
        "govome","globososo","sof-newhpnt","lp","v9m","gbanner","ddt-ff"};


}
