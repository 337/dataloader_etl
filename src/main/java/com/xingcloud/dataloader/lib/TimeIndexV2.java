package com.xingcloud.dataloader.lib;

import com.xingcloud.util.Constants;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * User: IvyTang
 * Date: 13-3-4
 * Time: 下午2:04
 */
public class TimeIndexV2 {

  private static final long T5min = 5 * 60 * 1000;

  private static final long sixtyDaysMillis  = 24*3600*1000*60l;

  private static final long twoDaysMillis =  24*3600*1000*2l;

  public static int getTimeIndex(long currentTS) {
    long timestamp = currentTS - T5min;
    return getGMTHour(timestamp) * 12 + getGMTMin(timestamp) / 5;
  }

  public static int getLastTimeIndex(long currentTS) {
    long timestamp = currentTS - T5min * 2;
    return getGMTHour(timestamp) * 12 + getGMTMin(timestamp) / 5;
  }

  public static int getYearMonthDay(long currentTS) {
    long timestamp = currentTS - T5min;
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
    df.setTimeZone(Constants.TMZ);
    Date now = new Date(timestamp);
    return Integer.parseInt(df.format(now));
  }

  public static int getLastTaskYearMonthDay(long currentTS) {
    long timestamp = currentTS - T5min * 2;
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
    df.setTimeZone(Constants.TMZ);
    Date now = new Date(timestamp);
    return Integer.parseInt(df.format(now));
  }


  private static int getGMTHour(long timestamp) {
    SimpleDateFormat df = new SimpleDateFormat("HH");
    df.setTimeZone(Constants.TMZ);
    Date now = new Date(timestamp);
    return Integer.parseInt(df.format(now));
  }

  private static int getGMTMin(long timestamp) {
    SimpleDateFormat df = new SimpleDateFormat("mm");
    df.setTimeZone(Constants.TMZ);
    Date now = new Date(timestamp);
    return Integer.parseInt(df.format(now));
  }

  public static String getSixtyDaysBefore(String nowDate) throws ParseException {

    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
    df.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    Date date = df.parse(nowDate);
    long dayStartTime = date.getTime();

    long sixtybefore = dayStartTime - sixtyDaysMillis;

    return df.format(new Date(sixtybefore));
  }

  public static String getTwoDaysBefore(String  nowDate) throws ParseException {
    SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
    df.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    Date date = df.parse(nowDate);
    long dayStartTime = date.getTime();

    long twoBefore = dayStartTime - twoDaysMillis;

    return df.format(new Date(twoBefore));

  }

  public static void main(String[] args) throws ParseException {
    System.out.println(getYearMonthDay(System.currentTimeMillis()));
    System.out.println(getTimeIndex(System.currentTimeMillis()));
    System.out.println(getLastTimeIndex(System.currentTimeMillis()));
    System.out.println(getSixtyDaysBefore("20130708"));
    System.out.println(getTwoDaysBefore("20130708"));
  }


}
