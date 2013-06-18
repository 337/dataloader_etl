package com.xingcloud.util;

import com.xingcloud.util.manager.DateManager;


import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Author: qiujiawei Date: 12-3-19
 */
public class Common {

    /**
     * 把时间戳转换为数据库格式日期
     * 格式：yyyyMMddHHmmss
     * @return
     */
    static public long getLongPresentByTimestamp(long timestamp){
        SimpleDateFormat df=new SimpleDateFormat("yyyyMMddHHmmss");
        df.setTimeZone(DateManager.TZ);
        Date date=new Date(timestamp);
        return Long.valueOf(df.format(date));
    }


}
