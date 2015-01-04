package com.xingcloud.util;

import com.xingcloud.mysql.MySqlDict;

import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

public class Constants {


    public static final char EVENT_SEPERATOR = '.';

    public static final TimeZone TMZ = TimeZone.getTimeZone("GMT+8");

    public static final int REDIS_DB_NUM = 0;

    public static final String MR_JOB_SQL_TABLE_PREFIX = "TBL_";
    public static final String MR_JOB_SQL_TABLE_COLD = MR_JOB_SQL_TABLE_PREFIX + "COLD";
    public static final String MR_JOB_SQL_TABLE_HOT = MR_JOB_SQL_TABLE_PREFIX + "HOT";


    public static final String DATE_FORMAT_FULL = "yyyyMMddHHmmss";
    public static final String DATE_FORMAT_SHORT = "yyyy-MM-dd";
    public static final String DATE_FORMAT_LONG = "yyyy-MM-dd HH:mm";
    public static final String DATE_FORMAT_MONTH = "yyyy-MM";
    public static final String DATE_FORMAT_SHORT_HOUR = "yyyy-MM-dd HH";

    //uid 缓存大小为每个项目300W
    public static final int CACHECAPACITY = 300 * 10000;
    public static final int CACAHECAPACITY_HUGE = 500 * 10000;
    public static final int RANDOMADUIDRANGE = 100 * 10000;

    public static final int HBASE_VERSION = 2000;

    public static final int FIRST_TASK_NUM = 0 ;
    public static final int LAST_TASK_NUM = 287;

    public static final long ONYDAY_TIMEMILLIS = 24 * 3600 * 1000l;


    public static final String SIXTY_DAYS_ACTIVE_USERS="/home/hadoop/60days_active_users/";
    public static final String SIXTY_DAYS_ACTIVE_USERS2="/home/hadoop/60days_test/";
//    public static final String SIXTY_DAYS_ACTIVE_USERS="/Users/ytang1989/Workspace/testfiles/hadoop" +
//        "/60days_active_users/";
    public static final String UID_CACHE_LOCAL_FILE_PREFIX = "/home/hadoop/uidcache_etl/uidcache_do_not_delete_";

    public static final String EVENTLOG = "/data/log/stream.log";
    public static final String USERLOG = "/data/log/user.log";
    public static final String DELAYLOG = "/data/log/delay.log";
    public static final String DELAYUSERLOG = "/data/log/delayuser.log";

    private static String[] HUGE_PROJECTS_ARRAY = new String[]{"v9-v9", "22find", "happyfarmer", "sof-dsk",
            "citylife", "sof-hpnt"};
    public static List<String> HUGE_PROJECTS = Arrays.asList(HUGE_PROJECTS_ARRAY);

    public static final String USER_DAYEND_LOG = "=======user day log end=======";

    public static final int OFFLINE_DB = 15;

    public static final String KEY_USERLOG = "userlog";

    public static final String KEY_MYSQLDUMP = "mysql_dump_status";

    public static final MySqlDict dict = MySqlDict.getInstance();

    public static final String LAST_LOGIN_TIME = "last_login_time";

    public static final String userColumnFamily = "v";

    public static final String userColumnQualifier = "v";

    public static final String ATTRIBUTE_TABLE = "user_attribute";

}
