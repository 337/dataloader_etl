package com.xingcloud.dataloader.buildtable.buildmysqltable;

import com.xingcloud.util.Constants;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class HotColdMultipleTextOutputFormat extends
        XMultipleOutputFormat<Text, NullWritable> {
    private static final String DISCARD = "DISCARD";
    private static SimpleDateFormat sdf = new SimpleDateFormat(
            Constants.DATE_FORMAT_SHORT);
    private static Date dayAgo30;
    static {
        sdf.setTimeZone(Constants.TMZ);
        Calendar c = Calendar.getInstance(Constants.TMZ);
        c.add(Calendar.DATE, -30);
        dayAgo30 = c.getTime();
    }

    @Override
    protected String generateFileNameForKeyValue(Text key, NullWritable value,
            String name) {
        if (key == null) {
            return DISCARD;
        }
        String line = key.toString();
        if (line == null || line.isEmpty()) {
            return DISCARD;
        }
        String[] array = line.split("\t");
        if (array == null || array.length < 3) {
            return DISCARD;
        }
        String lastLoginTimeString = array[2];
        if (lastLoginTimeString == null || "".equals(lastLoginTimeString) || "null".equals(lastLoginTimeString)) {
            return DISCARD;
        }

        Date lastLoginTime = null;
        try {
            lastLoginTime = sdf.parse(lastLoginTimeString);
        } catch (ParseException e) {
            e.printStackTrace();
            return DISCARD;
        }

        if (lastLoginTime.before(dayAgo30)) {
            return Constants.MR_JOB_SQL_TABLE_COLD;
        }
        return Constants.MR_JOB_SQL_TABLE_HOT;
    }

}
