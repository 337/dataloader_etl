package com.xingcloud.dataloader.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: IvyTang
 * Date: 13-4-23
 * Time: 下午5:58
 */
public class GeoIPCountryWhois {

  private static final Log LOG = LogFactory.getLog(GeoIPCountryWhois.class);

  static GeoIPCountryWhois instance = new GeoIPCountryWhois();

  Map<Long, String> ipCountryMap = new HashMap<Long, String>();
  List<Long> ipBlocks = new ArrayList<Long>();
  Map<Long, Long> ipStartEndBlocks = new HashMap<Long, Long>();
  //  List<Long> ipEndBlocks = new ArrayList<Long>();
  int startIpIndex = 5;
  int endIpIndex = 7;
  int countryIndex = 9;
  long ipStart = 16777216l;
  long ipEnd = 3758096383l;

  private GeoIPCountryWhois() {
    InputStream inputStream = this.getClass().getResourceAsStream(
            "/GeoIPCountryWhois.csv");
    BufferedReader bufferedReader = new BufferedReader(
            new InputStreamReader(inputStream));
    String tmp = null;
    try {
      while ((tmp = bufferedReader.readLine()) != null) {
        String[] items = tmp.split("\"");
        ipBlocks.add(Long.parseLong(items[startIpIndex]));

        ipStartEndBlocks.put(Long.parseLong(items[startIpIndex]), Long.parseLong(items[endIpIndex]));

        ipCountryMap.put(Long.parseLong(items[startIpIndex]), items[countryIndex]);
      }
    } catch (IOException e) {
      LOG.error("GeoIPCountryWhois init error", e);
    }
  }

  public static GeoIPCountryWhois getInstance() {
    return instance;
  }

  public String getCountry(long ipNumber) {

    if (ipNumber < ipStart || ipNumber > ipEnd)
      throw new IllegalArgumentException("ip range error,shoule be in [16777216,3758096383],not " + ipNumber);
    long startIp = getStartIPBlock(ipNumber, 0, ipCountryMap.size() - 1);
    long endIp = ipStartEndBlocks.get(startIp);
    if (ipNumber <= endIp)
      return ipCountryMap.get(startIp);
    else
      return null;
  }

  private Long getStartIPBlock(long targetIp, int lower, int upper) {
    if (upper - lower > 0) {
      int mid = (lower + upper) / 2;
      if (ipBlocks.get(mid) > targetIp)
        return getStartIPBlock(targetIp, lower, mid - 1);
      else if (ipBlocks.get(mid) < targetIp)
        return getStartIPBlock(targetIp, mid + 1, upper);
      else
        return targetIp;
    } else {
      if (ipBlocks.get(lower) > targetIp)
        return ipBlocks.get(lower - 1);
      else
        return ipBlocks.get(lower);
    }
  }

  public static void main(String[] args) {


    if (args.length != 0) {
      for (String arg : args) {
        System.out.println(arg);
        String[] ipItems = arg.split("\\.");
        long ipNumber = Long.valueOf(ipItems[0]) * 256 * 256 * 256 + Long.valueOf(ipItems[1])
                * 256 * 256 + Long.valueOf(ipItems[2]) * 256 + Long.valueOf(ipItems[3]);
        System.out.println(arg + "\t" + ipNumber + "\t" + GeoIPCountryWhois.getInstance().getCountry(ipNumber));
      }
    } else {
      String AUCountry = GeoIPCountryWhois.getInstance().getCountry(1180673756);

      if (!AUCountry.equals("US"))
        System.out.println("error ip 16777216,should be AU ,but is " + AUCountry);

      String CNCountry = GeoIPCountryWhois.getInstance().getCountry(16777472l);

      if (!CNCountry.equals("CN"))
        System.out.println("error ip 16777472,should be CN ,but is " + CNCountry);

      String THCountry = GeoIPCountryWhois.getInstance().getCountry(16819984l);

      if (!THCountry.equals("TH"))
        System.out.println("error ip 16819984,should be TH ,but is " + THCountry);

      String intranet = GeoIPCountryWhois.getInstance().getCountry(3232235619l);
      if (intranet != null)
        System.out.println("error in check intranet ip 3232235619");
      String intranet2 = GeoIPCountryWhois.getInstance().getCountry(3232301053l);
      if (intranet2 != null)
        System.out.println("error in check intranet ip 3232301055");

      String hole1 = GeoIPCountryWhois.getInstance().getCountry(86843393l);
      if (hole1 != null)
        System.out.println("error in check hole ip 86843393");

      String hole2 = GeoIPCountryWhois.getInstance().getCountry(406151267l);
      if (hole2 != null)
        System.out.println("error in check hole ip 406151267");

      String hole3 = GeoIPCountryWhois.getInstance().getCountry(1540353021l);
      if (hole3 != null)
        System.out.println("error in check hole ip 1540353021");

      String hole4 = GeoIPCountryWhois.getInstance().getCountry(3232235784l);
      System.out.println(hole4);


    }


  }


}
