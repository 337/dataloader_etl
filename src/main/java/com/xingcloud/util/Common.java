package com.xingcloud.util;

import com.xingcloud.util.manager.DateManager;


import java.text.SimpleDateFormat;
import java.util.*;
import com.cedarsoftware.util.SafeSimpleDateFormat;


/**
 * Author: qiujiawei Date: 12-3-19
 */
public class Common {

    static ThreadLocal<SimpleDateFormat> df1 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
            return df;
        }
    };

    public static ThreadLocal<SimpleDateFormat> df2 = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
            return df;
        }
    };

    /**
     * 把时间戳转换为数据库格式日期
     * 格式：yyyyMMddHHmmss
     * @return
     */
    static public long getLongPresentByTimestamp(long timestamp){
        Date date=new Date(timestamp);
        return Long.valueOf(df1.get().format(date));
    }


}
