package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.StaticConfig;

/**
 * Author: qiujiawei
 * Date:   12-3-12
 */
public class LocalPath {
  static public String SITE_DATA_LIST = StaticConfig.siteDataList;
  static public String STORE_LOG_LIST = StaticConfig.storeLogList;
  static public String V4_LOG_LIST = StaticConfig.v4LogList;
  static public int STANDARD_DATEFORMART_LENGTH = 8;
  static public String V4_LOG_FILENAME = "log_v4.";

  static public final String SITE_DATA = "site_data";
  static public final String STORE_LOG = "store_log";
  static public final String V4_LOG = "v4_log";

  static public String getPathWithPrefix(String prefix, String appid, String date, int index) {
    return prefix + "/" + appid + "/" + date.substring(0, 4) + "/" + date.substring(4, 6) + "/ea_data_" + date.substring(6, 8) + "/" + index + ".log";
  }

  static public String getV4LogPath(String v4LogPrefix, String date, int index) {
    if (date.length() != STANDARD_DATEFORMART_LENGTH)
      return null;
    String year = date.substring(0, 4);
    String month = date.substring(4, 6);
    String day = date.substring(6, 8);
    if (month.startsWith("0"))
      month = month.substring(1, 2);
    if (day.startsWith("0"))
      day = day.substring(1, 2);
    return v4LogPrefix + "/" + V4_LOG_FILENAME + year + "-" + month + "-" + day+"-"+index;
  }

  public static void main(String[] args){
    System.out.println(getV4LogPath(V4_LOG_LIST, "20130903", 123));
  }

}
