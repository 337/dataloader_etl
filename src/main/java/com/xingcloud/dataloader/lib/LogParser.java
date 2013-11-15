package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.util.ProjectInfo;
import com.xingcloud.util.manager.CurrencyManager;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;

/**
 * 解析日志，转化为event列表
 * Author: qiujiawei
 * Date:   12-3-12
 */
public class LogParser {
  static enum LogType {SITE_DATA, STORE_LOG, V4_LOG, DEFAULT}

  ;

  public static final Log LOG = LogFactory.getLog(LogParser.class);
  public static final int fieldLength = 32;

  private static final String UTM_SOURCE = "utm_source=";

  private LogType type = LogType.DEFAULT;

  public static long dayTimestampLength = 86400000L;
  public int wrongMax = 10;
  public int wrong = 0;
  private String log;
  private ProjectInfo projectInfo;
  private ObjectMapper objectMapper;

  private String xafrom_ref = "xafrom=";
  private String sgfrom_ref = "sgfrom=";
  private String facebook_age_ref_start = "{";

  private String geoip = "geoip";

  private int refSize = 5;
  private int rawRefColonSplitIndex = 2;

  public LogParser(String type) {
    init(type, null);
  }

  public LogParser(String type, ProjectInfo projectInfo) {
    init(type, projectInfo);
  }


  public void init(String type, ProjectInfo projectInfo) {
    this.projectInfo = projectInfo;
    this.objectMapper = new ObjectMapper();
    if (type.equals(LocalPath.SITE_DATA)) {
      this.type = LogType.SITE_DATA;
    } else if (type.equals(LocalPath.STORE_LOG)) {
      this.type = LogType.STORE_LOG;
    } else if (type.equals(LocalPath.V4_LOG)) {
      this.type = LogType.V4_LOG;
    }
  }

  /**
   * typo!!!
   *
   * @param inlog
   * @return
   * @deprecated
   */
  public List<Event> parser(String inlog) {
    return parse(inlog);
  }

  /**
   * 解析输入的log
   *
   * @param inlog log文本
   * @return 解析出来的事件列表
   */
  public List<Event> parse(String inlog) {
    try {
      log = inlog;
      if (type == LogType.SITE_DATA) return parseSite();
      else if (type == LogType.STORE_LOG) return parseStoreJackson();
      else if (type == LogType.V4_LOG) return parseV4Log();
      else return null;
    } catch (Exception e) {
      LOG.warn(log + e.getMessage(), e);
      if (wrong < wrongMax) {
        LOG.debug("parse is wrong:" + inlog, e);
      }
      wrong++;
    }
    return null;
  }

  /**
   * log format :project uid ref event {} timestamp
   *
   * @return the event list from the log
   */
  private List<Event> parseSite() throws IOException {


    List<Event> result = new ArrayList<Event>();

    String[] temp = log.split("\t");
    if (temp.length != 6) return null;
    String uid = temp[1];
    String[] event = new String[Event.eventFieldLength];
    String json = null;
    long value = 0;

    long ts = getTs(temp[5]);

    //针对特殊的事件，进行事件名称转换
    if (temp[3].equals("user.visit")) {
      event[0] = "visit";

    } else if (temp[3].equals("pay.complete")) {

      Map jsonObject = objectMapper.readValue(temp[4], Map.class);

      String channel = "NA";
      if (jsonObject.containsKey("channel")) {
        channel = jsonObject.get("channel").toString();
      }
      String fee = null;
      if (jsonObject.containsKey("fee")) {
        fee = jsonObject.get("fee").toString();
        //检测fee的正确性
        try {
          double feeDouble = Double.valueOf(fee);
        } catch (Exception efee) {
          LOG.error("fee not number." + log);
          fee = null;
        }
      }
      String gross = jsonObject.get("gross").toString();
      String currency = jsonObject.get("gcurrency").toString();

      event[0] = "pay";
      event[1] = "gross";
      event[2] = channel;

      long feeValue = 0;

      for (int i = 0; i < 3; i++) {
        try {
          value = CurrencyManager.calculateAmount(gross, currency);
          if (fee != null)
            feeValue = CurrencyManager.calculateAmount(fee, currency);
          break;
        } catch (Exception e) {
          if (i == 2) {
            LOG.error(log + " calculateAmount catch Exception" + gross + " " + currency, e);
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            LOG.error(e1.getMessage());
          }
        }
      }

      //add pay.fee.${channel}
      if (fee != null) {
        String[] feeEvents = new String[Event.eventFieldLength];
        feeEvents[0] = "pay";
        feeEvents[1] = "fee";
        feeEvents[2] = channel;
        result.add(new Event(uid, feeEvents, feeValue, ts, json));
      }
    } else if (temp[3].equals("user.update")) {
      event[0] = "update";
      //处理update情况下，json里面的ref
      json = rebuildUpdateJson(temp[4]);
    } else if (temp[3].equals("user.heartbeat")) {
      event[0] = "heartbeat";
    } else if (temp[3].equals("user.quit")) {
      event[0] = "quit";
    } else {
      String[] t = temp[3].split("\\.");
      //特殊规则：由于所有的默认事件都转入新版
      //pay.visit事件和pay.visitc事件都转入新版
      //导致和pay事件（原pay.complete)冲突                                                 1
      //所以转pay.visit为pay_platform.visit
      if (t.length >= 2) {
        if (t[0].equals("pay") && (t[1].equals("visit") || t[1].equals("visitc")))
          t[0] = "pay_platform";
      }
//      if (t[0].equals("pay") && (t[])) t[0] = "pay_platform";
      for (int i = 0; i < t.length && i < Event.eventFieldLength; i++) {
        event[i] = t[i];
      }
      //LOG.info(projectInfo.getProject()+": " +temp[3]);
      if (t.length == 0 || t[0].length() < 1) return null;
    }


    result.add(new Event(uid, event, value, ts, json));
    //所有pay事件的空uid和错误时间戳都打log
    if (event[0].equals("pay") && (uid.trim().length() == 0 || ts == 0)) {
      LOG.error("pay event error." + log);
    }
    //特殊规则 如果事件为visit事件，并 且appid为老式appid（ddt@facebook_pl_1)
    //将添加1个属性更新事件，更新language platform 和identifier
    if (event[0].equals("visit") && projectInfo != null) {
      Event updateEvent = getUpdateEvent(uid, value, ts);
      if (updateEvent != null)
        result.add(updateEvent);
      Event refUpdateEvent = getRefUpdateEvent(uid, temp[2], value, ts);
      if (refUpdateEvent != null)
        result.add(refUpdateEvent);
    }

    return result;
  }

  /**
   * 生成一个属性更新事件，更新language platform identifier
   *
   * @param uid   用户uid
   * @param value 事件值（没用）
   * @param ts    时间戳
   * @return 属性更新事件
   */
  private Event getUpdateEvent(String uid, long value, long ts) {
    String[] event = new String[Event.eventFieldLength];
    event[0] = "update";
    StringBuilder json = new StringBuilder();
    boolean first = true;

    json.append("{");
    if (projectInfo.getLanguage() != null) {
      if (first) first = false;
      else json.append(",");
      json.append("\"" + User.languageField + "\":\"").append(projectInfo.getLanguage()).append("\"");
    }
    if (projectInfo.getPlatform() != null) {
      if (first) first = false;
      else json.append(",");
      json.append("\"" + User.platformField + "\":\"").append(projectInfo.getPlatform()).append("\"");
    }
    if (projectInfo.getIdentifier() != null) {
      if (first) first = false;
      else json.append(",");
      json.append("\"" + User.identifierField + "\":\"").append(projectInfo.getIdentifier()).append("\"");
    }
    json.append("}");
    //System.out.println(json);
    return new Event(uid, event, value, ts, json.toString());
  }

  /**
   * visit  v4的visit事件如果appid是XX@XX_XX_XX的格式，一样提供language platform identifier的update事件
   *
   * @param uid
   * @param appid
   * @param value
   * @param ts
   * @return update事件
   */
  private Event getUpdateEventForV4(String uid, String appid, long value, long ts) throws IOException {
    String[] event = new String[Event.eventFieldLength];
    event[0] = "update";
    Map<String, String> updateMap = new HashMap<String, String>();
    ProjectInfo appidProjectInfo = ProjectInfo.getProjectInfoFromAppidOrProject(appid);
    if (appidProjectInfo.getLanguage() != null)
      updateMap.put(User.languageField, appidProjectInfo.getLanguage());
    if (appidProjectInfo.getIdentifier() != null)
      updateMap.put(User.identifierField, appidProjectInfo.getIdentifier());
    if (appidProjectInfo.getPlatform() != null)
      updateMap.put(User.platformField, appidProjectInfo.getPlatform());
    return new Event(uid, event, value, ts, objectMapper.writeValueAsString(updateMap));
  }


  private String rebuildUpdateJson(String json) throws IOException {
    Map updateMap = objectMapper.readValue(json, Map.class);
    // JSONObject jsonObject = JSONObject.fromObject(json);

    if (updateMap.containsKey("ref")) {
      String refValue = (String) updateMap.get("ref");
      Map<String, String> refAnalyseValue = analyseRef(refValue.trim());
      updateMap.remove("ref");
      for (Map.Entry<String, String> entry : refAnalyseValue.entrySet())
        updateMap.put(entry.getKey(), entry.getValue());
    }
    //local的属性，传入的是ip的大小，转为相应的国家
    if (updateMap.containsKey(geoip)) {
      try {
        long ipNumber = Long.parseLong(updateMap.get(geoip).toString());
        String country = GeoIPCountryWhois.getInstance().getCountry(ipNumber);
        if (country != null)
          updateMap.put(geoip, country);
        else
          updateMap.remove(geoip);
      } catch (NumberFormatException e) {
        updateMap.put(geoip, updateMap.get(geoip).toString());
      }
    }
    return objectMapper.writeValueAsString(updateMap);
  }

  //visit事件，也更新ref的状态
  private Event getRefUpdateEvent(String uid, String refContent, long value, long ts) throws IOException {
    refContent = refContent.trim();
    String[] event = new String[Event.eventFieldLength];
    event[0] = "update";
    Map<String, String> refs = analyseRef(refContent);
    if (refs == null || refs.isEmpty())
      return null;
    return new Event(uid, event, value, ts, objectMapper.writeValueAsString(refs));
  }

  //分析ref字段，以xafrom=开头，且xafrom=之后有值，则放入ref0-ref4的5个属性  ;如果以sgfrom=开头，则更新到ref属性里
  //xafrom的处理，如果存在*号，取*号之后的内容的第二个；号之后的的内容；没有*号，就直接取第二个；号的以后的内容
  private Map<String, String> analyseRef(String refContent) {
    refContent = refContent.trim();
    Map<String, String> refs = new HashMap<String, String>();
    if (refContent.startsWith(xafrom_ref)) {
      if (refContent.length() > xafrom_ref.length()) {
        int lastStarIndex = refContent.lastIndexOf("*");
        String[] refTmps = null;
        if (lastStarIndex == -1)
          refTmps = refContent.substring(xafrom_ref.length()).split(";");
        else
          refTmps = refContent.substring(lastStarIndex + 1).split(";");

        if (refTmps.length <= rawRefColonSplitIndex) {
          String xaFromValue = refContent.substring(xafrom_ref.length());
          if (xaFromValue.startsWith(UTM_SOURCE)) {
            // 处理这种情况： xafrom=utm_source=tapjoy
            refs.put("ref0", xaFromValue.substring(UTM_SOURCE.length()).trim());
          } else {
            refs.put("ref", xaFromValue);
          }
        }
        for (int i = rawRefColonSplitIndex; i < refTmps.length; i++) {
          if (i < rawRefColonSplitIndex + refSize - 1) {
            if (refTmps[i].trim().length() > 0)
              refs.put("ref" + (i - rawRefColonSplitIndex), refTmps[i].trim());
          } else {
            String content = refs.get("ref" + (refSize - 1));
            if (content == null)
              refs.put("ref" + (refSize - 1), refTmps[i].trim());
            else
              refs.put("ref" + (refSize - 1), content + ";" + refTmps[i].trim());
          }
        }
        String finalRef4 = refs.get("ref" + (refSize - 1));
        if (finalRef4 != null) {
          if (finalRef4.trim().length() == 0 || finalRef4.trim().replaceAll(";", "").length() == 0)
            refs.remove("ref" + (refSize - 1));
        }
      }

    } else if (refContent.startsWith(sgfrom_ref)) {
      if (refContent.length() > sgfrom_ref.length())
        refs.put("ref", refContent.substring(sgfrom_ref.length()).trim());
    } else if (refContent.startsWith(facebook_age_ref_start)) {
      try {
        Map facebook_ref_age_map = objectMapper.readValue(refContent, Map.class);
        if(facebook_ref_age_map.containsKey("app") && facebook_ref_age_map.containsKey("t"))
          refs.put("ref0","f");
      } catch (IOException e) {
        LOG.warn(log, e);
        refs.put("ref", refContent);
      }
    } else {
      if (refContent.length() > 0)
        refs.put("ref", refContent);
    }
    return refs;
  }

  /**
   * log format :
   * {"signedParams":{"appid":"appid/projectid","uid":"uid"},"stats":[{"timestamp":1343177269,"data":["ad","click","","","","",1],"statfunction":"count"}]}
   * 兼容有些版本data可能为hash的结构
   * {"signedParams":{"appid":"appid/projectid","uid":"uid"},"stats":[{"timestamp":1343177269,"data":{"level_1":"ad"，"level_2":"click"},"statfunction":"count"}]}
   * <p/>
   * <p/>
   * example：
   * {"signedParams":{"appid":"v9-gdp","uid":"WD-WMAP9H468677_WDCWD1600AABS-61PRA0"},"stats":[{"timestamp":1343177269,"data":["ad","click","","","","",1],"statfunction":"count"}]}
   * {"signedParams":{"appid":"v9-gdp","uid":"WD-WMAP9H468677_WDCWD1600AABS-61PRA0"},"stats":[{"timestamp":1343177269,"data":{"level_1":"ad"，"level_2":"click"},"statfunction":"count"}]}
   *
   * @return the event list from the log
   */
  @Deprecated
  private List<Event> parseStore() {
    List<Event> result = new ArrayList<Event>();
    JSONObject json = JSONObject.fromObject(log);
    String uid = null;
    String[] eventStr;
    long value = 0;
    long timestamp;
    String signedParamsTimestamp = null;
    if (json.getJSONObject("signedParams").has("sns_uid")) {
      uid = json.getJSONObject("signedParams").getString("sns_uid");
    } else {
      uid = json.getJSONObject("signedParams").getString("uid");
    }
    if (json.getJSONObject("signedParams").has("timestamp")) {
      signedParamsTimestamp = json.getJSONObject("signedParams").getString("timestamp");
    }
    JSONArray stats = json.getJSONArray("stats");
    for (int i = 0; i < stats.size(); i++) {
      eventStr = new String[6];
      JSONObject event = stats.getJSONObject(i);
      String statFunction = null;
      //兼容有些版本叫做statfunction，有些叫做function
      if (event.containsKey("statfunction"))
        statFunction = event.getString("statfunction");
      else if (event.containsKey("function"))
        statFunction = event.getString("function");
      if (statFunction.equals("Count") || statFunction.equals("count")) {
        Object data = event.get("data");
        if (data instanceof JSONArray) {
          JSONArray eventAll = (JSONArray) data;
          if (eventAll.size() != 7) continue;
          for (int j = 0; j < Event.eventFieldLength; j++) {
            String temp = StaticFunction.ensureLength(eventAll.getString(j).replace(".", "_"), fieldLength);
            eventStr[j] = temp.trim();
          }
          value = eventAll.getLong(6);
        }
        //兼容有些版本data可能为hash的结构
        else if (data instanceof JSONObject) {
          JSONObject eventAll = (JSONObject) data;
          if (eventAll.containsKey("type")) {
            eventStr[0] = eventAll.getString("type");
          }
          for (int levelIndex = 1; levelIndex <= 5; levelIndex++) {
            if (eventAll.containsKey("level_" + levelIndex)) {
              String temp = StaticFunction.ensureLength(eventAll.getString("level_" + levelIndex).replace(".", "_"), fieldLength);
              eventStr[levelIndex] = temp.trim();
            }
          }
          if (eventAll.containsKey("count")) {
            value = eventAll.getLong("count");
          }
        }
      }
      //处理buyitem的日志
      else if (statFunction.equals("buyitem")) {
        Object data = event.get("data");

        JSONArray eventAll = event.getJSONArray("data");
        if (data instanceof JSONArray) {
          eventAll = (JSONArray) data;
        } else
          eventStr[0] = "buyitem";
        for (int j = 0; j < 5; j++) {
          String temp = StaticFunction.ensureLength(eventAll.getString(j).replace(".", "_"), fieldLength);
          if (temp.length() > fieldLength) temp = temp.substring(0, fieldLength);
          eventStr[j + 1] = temp;
        }
        value = eventAll.getLong(eventAll.size() - 1);
      }
            /*
               兼容历史的milestone类型的日志，转为普通的action.

               历史的milestone的用法：
                   "data":["mission_11","","","","","",1]
               转换以后，成为：
                   "action": milestone.mission.11

                   "data":["mission_11","s1","s2","s3","s4","s5",1]
               转换以后，成为：
                   "action": milestone.mission.11.s1.s2.s3.s4.s5
               用于action最多支持6层，这里变为：
                   milestone.mission.11.s1.s2.s3

               */
      else if (statFunction.equals("Milestone")) {
        JSONArray eventAll = event.getJSONArray("data");
        eventStr[0] = "milestone";
        if (eventAll.size() > 0) {
          String m0 = eventAll.getString(0);
          StringTokenizer st = new StringTokenizer(m0, "_");
          int j = 1;
          for (j = 1; j <= 5; j++) {
            if (!st.hasMoreTokens()) {
              break;
            }
            eventStr[j] = st.nextToken();
          }
          if (j <= 5) {
            for (int k = 1; k < eventAll.size() - 1; k++) {
              eventStr[j++] = eventAll.getString(k);
              if (j > 5) {
                break;
              }
            }
          }
        }
        value = eventAll.getLong(eventAll.size() - 1);
      }
      //其余抛弃
      else continue;

      if (event.containsKey("timestamp")) {
        timestamp = getTs(event.getString("timestamp"));
      } else {
        timestamp = getTs(signedParamsTimestamp);
      }

      //添加时间到最终的事件列表
      //防止第一层是空的event
      if (eventStr[0].length() >= 1) {
        result.add(new Event(uid, eventStr, value, timestamp));
      }
    }
    return result;

  }


  //parse store log using jackson
  private List<Event> parseStoreJackson() throws IOException {
    List<Event> result = new ArrayList<Event>();
    Map json = objectMapper.readValue(log, Map.class);
    String uid = null;
    String[] eventStr;
    long value = 0;
    long timestamp;
    String signedParamsTimestamp = null;
    Map signedParamsMap = (Map) json.get("signedParams");
    if (signedParamsMap.containsKey("sns_uid")) {
      uid = (String) signedParamsMap.get("sns_uid");
    } else {
      uid = (String) signedParamsMap.get("uid");
    }
    if (signedParamsMap.containsKey("timestamp"))
      signedParamsTimestamp = signedParamsMap.get("timestamp").toString();

    List stats = (List) json.get("stats");
    for (Object stat : stats) {
      eventStr = new String[6];
      Map event = (Map) stat;
      String statFunction = null;
      if (event.containsKey("statfunction"))
        statFunction = (String) event.get("statfunction");
      else if (event.containsKey("function"))
        statFunction = (String) event.get("function");
      if ("Count".equals(statFunction) || "count".equals(statFunction)) {
        Object data = event.get("data");
        if (data instanceof List) {
          List eventAll = (List) data;
          if (eventAll.size() != 7) continue;
          for (int j = 0; j < Event.eventFieldLength; j++) {
            if (eventAll.get(j) == null)
              eventStr[j] = "NA";
            else {
              String temp = StaticFunction.ensureLength(eventAll.get(j).toString().replace(".", "_"), fieldLength);
              eventStr[j] = temp.trim();
            }
          }
          if (eventAll.get(6).toString().length() != 0) {
            value = Long.parseLong(eventAll.get(6).toString());
          }
        } else if (data instanceof Map) {
          Map eventAll = (Map) data;
          if (eventAll.containsKey("type")) {
            eventStr[0] = (String) eventAll.get("type");
          }
          for (int levelIndex = 1; levelIndex <= 5; levelIndex++) {
            if (eventAll.containsKey("level_" + levelIndex)) {
              String temp = StaticFunction.ensureLength(eventAll.get("level_" + levelIndex).toString().replace(".", "_"), fieldLength);
              eventStr[levelIndex] = temp.trim();
            }
          }
          if (eventAll.containsKey("count")) {
            if (eventAll.get("count").toString().length() != 0)
              value = Long.parseLong(eventAll.get("count").toString());
          }
        }

      } else if ("buyitem".equals(statFunction)) {
        Object data = event.get("data");
        if (data instanceof Map) {
          Map eventAll = (Map) data;
          eventStr[0] = "buyitem";
          if (eventAll.containsKey("resource"))
            eventStr[1] = eventAll.get("resource").toString();
          else
            eventStr[1] = "NA";
          if (eventAll.containsKey("paytype"))
            eventStr[2] = eventAll.get("paytype").toString();
          else
            eventStr[2] = "NA";
          for (int levelIndex = 1; levelIndex <= 3; levelIndex++) {
            if (eventAll.containsKey("level_" + levelIndex)) {
              eventStr[levelIndex + 2] = StaticFunction.ensureLength(eventAll.get("level_" + levelIndex)
                      .toString().replace(".", "_"), fieldLength).trim();
            } else {
              eventStr[levelIndex + 2] = "NA";
            }
          }
          int times = eventAll.containsKey("number") ? Integer.parseInt(eventAll.get("number").toString()) : 1;
          long amount = eventAll.containsKey("amount") ? Long.parseLong(eventAll.get("amount").toString()) : 0l;
          value = amount * times;
        } else if (data instanceof List) {
          List eventAll = (List) data;
          eventStr[0] = "buyitem";
          if (eventAll.size() == 8) {
            for (int i = 0; i < 5; i++) {
              eventStr[i + 1] = eventAll.get(i).toString();
            }
            value = Long.parseLong(eventAll.get(7).toString());
          } else if (eventAll.size() == 9) {
            for (int i = 0; i < 5; i++) {
              eventStr[i + 1] = eventAll.get(i).toString();
            }
            value = Long.parseLong(eventAll.get(7).toString()) * Integer.parseInt(eventAll.get(8).toString());
          }
        }
        //milestone
      } else if ("Milestone".equals(statFunction) || "milestone".equals(statFunction)) {
        Object data = event.get("data");
        if (data instanceof Map) {
          Map eventAll = (Map) data;
          eventStr[0] = "milestone";
          eventStr[1] = eventAll.containsKey("milestone_name") ? eventAll.get("milestone_name").toString()
                  : "NA";
        } else if (data instanceof List) {
          List eventAll = (List) data;
          eventStr[0] = "milestone";
          eventStr[1] = eventAll.get(0).toString();
        }
        value = 1l;
      }
      if (event.containsKey("timestamp")) {
        timestamp = getTs(event.get("timestamp").toString());
      } else {
        timestamp = getTs(signedParamsTimestamp);
      }
      //添加时间到最终的事件列表
      //防止第一层是空的event
      if (eventStr[0] != null && eventStr[0].length() >= 1) {
        result.add(new Event(uid, eventStr, value, timestamp));
      }
    }

    return result;
  }

  private List<String> splitUsingGivenStr(String splitParent, String splitStr) {
    List<String> temps = new ArrayList<String>();
    int start = 0;
    int pos = -1;
    while ((pos = splitParent.indexOf(splitStr, start)) > -1) {
      temps.add(splitParent.substring(start, pos));
      start = pos + 1;
    }
    if (start < splitParent.length()) {
      temps.add(splitParent.substring(start));
    }
    return temps;
  }

  private List<Event> parseV4Log() throws Exception {
    List<Event> results = new ArrayList<Event>();

    List<String> temps = splitUsingGivenStr(log, "\t");

    //update log :age	uid123	update	platform	androidmarket	1378189200000
    if (temps.size() == StaticConfig.V4_UPDATE_ITEMS_NUM && temps.get(2).equals("update")) {
      Map<String, String> updatePropertyMap = new HashMap<String, String>();
      if (temps.get(3).equals("ref")) {
        updatePropertyMap = analyseRef(temps.get(4).trim());
      } else if (temps.get(3).equals("geoip")) {
        try {
          long ipNumber = Long.parseLong(temps.get(4));
          String country = GeoIPCountryWhois.getInstance().getCountry(ipNumber);
          if (country != null)
            updatePropertyMap.put(geoip, country);
        } catch (NumberFormatException e) {
          updatePropertyMap.put(geoip, temps.get(4));
        }
      } else {
        updatePropertyMap.put(temps.get(3), temps.get(4));
      }
      String[] eventArray = new String[Event.eventFieldLength];
      eventArray[0] = "update";
      long ts = getTs(temps.get(5));
      results.add(new Event(temps.get(1), eventArray, 0, ts, objectMapper.writeValueAsString(updatePropertyMap)));
    } else if (temps.size() == StaticConfig.V4_ACTION_ITEMS_NUM) {

      String[] eventArray = new String[Event.eventFieldLength];
      long ts = getTs(temps.get(4));

      //visit event:age@gg_en_android.global.s60	353976050898489-38AA3C2BFC89	visit	0	1378716358447
      if (temps.get(2).equals("visit")) {
        eventArray[0] = "visit";
        //visit
        results.add(new Event(temps.get(1), eventArray, 0, ts));
        results.add(getUpdateEventForV4(temps.get(1), temps.get(0), 0, ts));
        //pay.gross || pay.fee event:
      } else if (temps.get(2).startsWith("pay.gross") || temps.get(2).startsWith("pay.fee")) {
        List<String> t = splitUsingGivenStr(temps.get(2), ".");
        for (int i = 0; i < t.size() && i < Event.eventFieldLength; i++) {
          eventArray[i] = t.get(i);
        }
        results.add(new Event(temps.get(1), eventArray, CurrencyManager.calculateAmount(temps.get(3), "USD"), ts));
      } else {
        List<String> t = splitUsingGivenStr(temps.get(2), ".");
        if (t.size() > 1) {
          if (t.get(0).equals("pay") && (t.get(1).equals("visit") || t.get(1).equals("visitc")))
            t.set(0, "pay_platform");
        }
        for (int i = 0; i < t.size() && i < Event.eventFieldLength; i++) {
          eventArray[i] = t.get(i);
        }
        results.add(new Event(temps.get(1), eventArray, Long.parseLong(temps.get(3)), ts));
      }
    }
    return results;
  }


  /**
   * 时间戳规则处理10位或者13位时间戳，如果
   * 日志时间戳大于当前时间戳，修改为当前时间戳
   * 由于,php层做了时间戳处理，V4只能按照10位时间戳(精确到秒）存储
   *
   * @param timestamp 时间戳字符串
   * @return 转换后的时间戳
   */
  public static long getTs(String timestamp) {
        /*
       add delay deal
        */
    long temp = 0;
    long now = System.currentTimeMillis();
    try {
      if (timestamp.length() == 10) {
        temp = Long.parseLong(timestamp) * 1000;
      } else if (timestamp.length() == 13) {
        temp = Long.parseLong(timestamp);
      }
      if (temp > now) {
        temp = now;
      }
      return temp;
    } catch (Exception e) {
      LOG.debug("ts is wrong:" + timestamp);
    }
    return now;
  }

  public static void main(String[] args) throws IOException {
    LogParser lp = new LogParser("site_data", null);
    String refLog1 = "xafrom=n=C*k=*c=32069512676*s=www.oneonlinegames.com*br;ddt;g;c;Content-KT2;ddt50lp2";
    String refLog2 = "xafrom=utm_source=tapjoy";

    System.out.println(lp.analyseRef(refLog1));
    System.out.println(lp.analyseRef(refLog2));
  }

}
