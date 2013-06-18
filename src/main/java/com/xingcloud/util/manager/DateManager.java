package com.xingcloud.util.manager;

import com.xingcloud.util.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

/**
 * Author: qiujiawei Date: 11-7-14
 * 
 * 1.init this class to ond day
 * 
 * DateManager.newInstance(DateManager.getOneDay(-1)); //yesterday
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 */
public class DateManager {
    public static final Log LOG = LogFactory.getLog(DateManager.class);

    private String date;
    private String weekPeriod;
    private String monthPeriod;
    private long beginTimestamp;
    private long endTimestamp;
    public static final TimeZone TZ = Constants.TMZ;

    private DateManager(String date) {
        init(date);
    }

    private void init(String date) {
        this.date = date;
        weekPeriod = DateManager.getTheWeek(date);
        monthPeriod = DateManager.getTheMonth(date);
        beginTimestamp = DateManager.getTimestamp(date);
        endTimestamp = beginTimestamp + 60 * 60 * 24 * 1000;
    }

    private static DateManager instance;

    private static DateManager getInstance() {
        if (instance == null)
            instance = new DateManager(DateManager.getOneDay(-1));
        return instance;
    }

    public static void newInstance(String date) {
        instance = new DateManager(date);
    }

    public static boolean checkDate(String date) {
        return date != null && date.matches("^[0-9]{4}-[0-9]{2}-[0-9]{2}");
    }

    // ************************
    public static int DayDis(String begin, String end) {
        long re = 0;
        try {
            Date a = new Date(DateManager.getTimestamp(begin));
            Date b = new Date(DateManager.getTimestamp(end));
            re = a.getTime() - b.getTime();
            re /= (3600000 * 24);
            // if (re < 0)
            // re = -re;
        } catch (Exception e) {
            LOG.error("DateManager.daydis catch Exception with params is "
                    + begin + " " + end, e);
        }
        return (int) re;
    }

    public static String calDay(String date, int dis) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        sdf.setTimeZone(Constants.TMZ);
        Date temp = new Date(DateManager.getTimestamp(date));

        java.util.Calendar ca = Calendar.getInstance(new SimpleTimeZone(0,
                Constants.TMZ.getID()));
        ca.setTime(temp);
        ca.add(Calendar.DAY_OF_MONTH, dis);
        return sdf.format(ca.getTime());
    }

    public static int getGMTHour() {
        SimpleDateFormat df = new SimpleDateFormat("HH");
        Calendar cal = Calendar.getInstance(new SimpleTimeZone(0,Constants.TMZ.getID()));
        df.setCalendar(cal);
        Date now = Calendar.getInstance().getTime();
        int hour = Integer.parseInt(df.format(now));
        return hour;
    }

    public static String nowtime() {
        String ID = "HongKong";
        TimeZone tz = TimeZone.getTimeZone(ID);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");// DateFormat.getInstance();
        df.setTimeZone(tz);
        java.sql.Date now = new java.sql.Date(System.currentTimeMillis());
        return df.format(now);
    }

    public static String TimeDis(long begintime, long endtime) {
        long second = endtime - begintime;
        second /= 1000;
        long min = second / 60;
        second %= 60;
        return " " + min + " min " + second + " sec";
    }

    public static String TimeDis2(long millisecond) {
        long hour = millisecond / 60 / 60 / 1000;
        long min = (millisecond - hour * 60 * 60 * 1000) / 60 / 1000;
        long second = (millisecond - hour * 60 * 60 * 1000 - min * 60 * 1000) / 1000;
        long milli = (millisecond - hour * 60 * 60 * 1000 - min * 60 * 1000 - second * 1000);
        return hour + ":" + String.format("%02d", min) + ":"
                + String.format("%02d", second) + "."
                + String.format("%03d", milli);
    }

    public static String getUsingTime(long total) {
        long second = total / 1000;
        long min = second / 60;
        second %= 60;
        return " " + min + " min " + second + " sec";
    }

    public static String getTheWeek(String date) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        sdf.setTimeZone(Constants.TMZ);
        Date temp = new Date(DateManager.getTimestamp(date));
        Calendar ca = Calendar.getInstance();
        ca.setTime(temp);
        ca.set(Calendar.DAY_OF_WEEK, 1);
        String beginDate = sdf.format(ca.getTime());
        ca.set(Calendar.DAY_OF_WEEK, 7);
        String endDate = sdf.format(ca.getTime());
        return beginDate + " to " + endDate;
    }

    public static String getTheMonth(String date) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM");
        sdf.setTimeZone(Constants.TMZ);
        Date temp = new Date(DateManager.getTimestamp(date));
        return sdf.format(temp);
    }

    public static long getTimestamp(String date) {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");// DateFormat.getInstance();
        df.setTimeZone(Constants.TMZ);
        String dateString = date + " 00:00:00";
        Date nowDate = null;
        try {
            nowDate = df.parse(dateString);
        } catch (ParseException e) {
            LOG.error("DateManager.daydis catch Exception with params is "
                    + date, e);
        }
        if (nowDate != null) {
            return nowDate.getTime();
        } else {
            return -1;
        }
    }

    public static String getOneDay(long dis) {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");// DateFormat.getInstance();
        df.setTimeZone(Constants.TMZ);
        long oneDay = 24 * 60 * 60 * 1000;
        Date yesterday = new Date(System.currentTimeMillis() + dis * oneDay);
        return df.format(yesterday);
    }

    public static String getDateFromTimestamp(Long timestamp) {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");// DateFormat.getInstance();
        df.setTimeZone(Constants.TMZ);
        Date yesterday = new Date(timestamp);
        return df.format(yesterday);
    }

    public static String getDate() {
        return DateManager.getInstance().date;
    }

    public static boolean inWeek(String judgeDate) {
        if (judgeDate == null || !checkDate(judgeDate))
            return false;
        String week = getTheWeek(judgeDate);
        if (week.compareTo(DateManager.getInstance().weekPeriod) == 0)
            return true;
        else
            return false;
    }

    public static boolean inMonth(String judgeDate) {
        if (judgeDate == null || !checkDate(judgeDate))
            return false;
        String month = getTheMonth(judgeDate);
        if (month.compareTo(DateManager.getInstance().monthPeriod) == 0)
            return true;
        else
            return false;
    }

    public static int compareDate(String DATE1, String DATE2) {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(Constants.TMZ);
        try {
            Date dt1 = df.parse(DATE1);
            Date dt2 = df.parse(DATE2);
            if (dt1.getTime() > dt2.getTime()) {
                return 1;
            } else if (dt1.getTime() < dt2.getTime()) {
                return -1;
            } else {
                return 0;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return 0;
    }

    public static Date short2Date(String date) throws ParseException {

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(Constants.TMZ);
        return df.parse(date);
    }

    public static int parseYear(String date) throws ParseException {

        SimpleDateFormat df = new SimpleDateFormat("yyyy");
        df.setTimeZone(Constants.TMZ);
        return Integer.parseInt(df.format(short2Date(date)));
    }

    public static int parseMonth(String date) throws ParseException {

        SimpleDateFormat df = new SimpleDateFormat("MM");
        df.setTimeZone(Constants.TMZ);
        return Integer.parseInt(df.format(short2Date(date)));
    }

    public static int parseDate(String date) throws ParseException {

        SimpleDateFormat df = new SimpleDateFormat("dd");
        df.setTimeZone(Constants.TMZ);
        return Integer.parseInt(df.format(short2Date(date)));
    }

    public static String dateAdd(String date, int offest) throws ParseException {
        if (offest == 0) {
            return date;
        }

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");// DateFormat.getInstance();
        df.setTimeZone(Constants.TMZ);
        long oneDay = 24 * 60 * 60 * 1000;
        Date yesterday = new Date(df.parse(date).getTime() + offest * oneDay);
        return df.format(yesterday);
    }

    public static String dateSubtraction(String date, int offest)
            throws ParseException {
        return dateAdd(date, -offest);
    }

    public static void main(String[] args) throws ParseException {
        String date = "2012-03-30";
        System.out.println(dateAdd(date, 3));
        System.out.println(dateSubtraction(date, 3));
    }

}
