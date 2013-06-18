package com.xingcloud.dataloader.lib;

import com.xingcloud.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Author: qiujiawei
 * Date:   12-3-12
 */
public class TimeIndex {
    static private long timestamp = 0;
    static private String date = null;
    static private int index = -1;
    static private final long T5min = 5 * 60 * 1000;
    static private final TimeZone tz = Constants.TMZ;

    static public void init() {
        timestamp = System.currentTimeMillis() - T5min;
        init(timestamp);
    }

    static public void init(long ts) {
        timestamp = ts;
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        df.setTimeZone(tz);
        Date now = new Date(timestamp);
        TimeIndex.date = df.format(now);
        index = getGMTHour() * 12 + getGMTMin() / 5;
    }

    static public void init(String date, int index) {
        TimeIndex.date = date;
        TimeIndex.index = index;
    }

    static public int getTimeIndex() {
        if (index == -1) {
            init();
        }
        return index;
    }

    static public int getTimeIndex(long getTimeIndex) {
        timestamp = getTimeIndex;
        return getTimeIndex();
    }

    static private int getGMTHour() {
        SimpleDateFormat df = new SimpleDateFormat("HH");
        df.setTimeZone(tz);
        Date now = new Date(timestamp);
        return Integer.parseInt(df.format(now));
    }

    static private int getGMTMin() {
        SimpleDateFormat df = new SimpleDateFormat("mm");
        df.setTimeZone(tz);
        Date now = new Date(timestamp);
        return Integer.parseInt(df.format(now));
    }

    static public String getDate() {
        if (date == null) {
            init();
        }
        return date;
    }
}
