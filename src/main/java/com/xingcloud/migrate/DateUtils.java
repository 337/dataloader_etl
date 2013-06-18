package com.xingcloud.migrate;

import com.xingcloud.util.Constants;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtils {
    public static String getPattern(Calendar c, TimeZone tz, boolean yesterday) {
        // TimeZone tz = TimeZone.getTimeZone("GMT-01:00");
        c.setTimeZone(tz);
        if (yesterday) {
            c.add(Calendar.DATE, -1);
        }
        SimpleDateFormat sdfYear = new SimpleDateFormat("yyyy");
        SimpleDateFormat sdfMonth = new SimpleDateFormat("MM");
        SimpleDateFormat sdfDate = new SimpleDateFormat("dd");
        sdfYear.setTimeZone(tz);
        sdfMonth.setTimeZone(tz);
        sdfDate.setTimeZone(tz);
        String currentYear = sdfYear.format(c.getTime());
        String currentMonth = sdfMonth.format(c.getTime());
        String currentDate = sdfDate.format(c.getTime());
        String pattern = currentYear + File.separatorChar + currentMonth
                + File.separatorChar + "ea_data_" + currentDate;
        return pattern;
    }

    public static long getMin5Number(Calendar c, TimeZone tz) {
        c.setTimeZone(tz);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(tz);
        System.out.println(sdf.format(c.getTime()));
        long current = c.getTimeInMillis();
        int year = c.get(Calendar.YEAR);
        int month = c.get(Calendar.MONTH);
        int date = c.get(Calendar.DATE);

        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        long currentDay = c.getTimeInMillis();
        long distance = current - currentDay;
        if (distance <= 5 * 60 * 1000) {
            return 287;
        }
        return distance / (5 * 60 * 1000) - 1;
    }

    public static void main(String[] args) {
        TimeZone tz = Constants.TMZ;
        Calendar c = Calendar.getInstance();

        long min5Number = DateUtils.getMin5Number(c, tz);
        String pattern = null;
        if (min5Number == 287) {
            pattern = DateUtils.getPattern(c, tz, true);
        } else {
            pattern = DateUtils.getPattern(c, tz, false);
        }
        System.out.println(pattern + "\\" + min5Number + ".log");
    }


    public static String getYesterday(String today) throws ParseException {
        SimpleDateFormat inputSimpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        Date todayDate = inputSimpleDateFormat.parse(today);
        long yesterdayTime = todayDate.getTime() - 24 * 60 * 60 * 1000;
        Date yesterday = new Date(yesterdayTime);
        String yesterdayDate = inputSimpleDateFormat.format(yesterday);

        return yesterdayDate;
    }
}
