package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.hbase.table.user.UserPropertyBitmaps;

import com.xingcloud.mysql.UserProp;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Author: qiujiawei
 * Date:   12-3-21
 */
public class User {

  private static final Log LOG = LogFactory.getLog(User.class);


  private long seqUid;
  private long samplingSeqUid;


  public Map<String, Object> property = new HashMap<String, Object>();

  static public final String specialUid1 = "123456";
  static public final String specialUid2 = "random";


  static public boolean isSpecialUid(String key) {
    String lower = key.toLowerCase();
    return lower.equals(specialUid1) || lower.equals(specialUid2);
  }

  public User(long seqUid) {
    this.seqUid = seqUid;
    this.samplingSeqUid = UidMappingUtil.getInstance().decorateWithMD5(seqUid);
  }

  //    public  Object getProperty(String key){
//        return property.get(key);
//    }
  public Long getLongProperty(String key) {
    Object re = property.get(key);
    if (re != null && re instanceof Long) {
      return (Long) re;
    }
    return null;
  }

  public Integer getIntegerProperty(String key) {
    Object re = property.get(key);
    if (re != null && re instanceof Integer) {
      return (Integer) re;
    }
    return null;
  }

  public String getStringProperty(String key) {
    Object re = property.get(key);
    if (re != null && re instanceof String && ((String) re).length() > 0) {
      return (String) re;
    }
    return null;
  }

  public boolean containsKey(String key) {
    return property.containsKey(key);
  }

  public void incProperty(String key, Integer value) {
    Integer temp = getIntegerProperty(key);
    if (temp == null) temp = 0;
    property.put(key, temp + value);

  }

  public void updateProperty(String key, Object value) {
    property.put(key, value);
  }

  public void removeProperty(String key) {
    property.remove(key);
  }

  public Set<Map.Entry<String, Object>> getPropertyEntrySet() {
    return property.entrySet();
  }

  public int propertySize() {
    return property.size();
  }


  static public final String registerField = "register_time";
  static public final String lastLoginTimeField = "last_login_time";
  static public final String firstPayTimeField = "first_pay_time";
  static public final String lastPayTimeField = "last_pay_time";

  static public final String gradeField = "grade";
  static public final String gameTimeField = "game_time";
  static public final String payAmountField = "pay_amount";

  static public final String languageField = "language";
  static public final String versionField = "version";
  static public final String platformField = "platform";
  static public final String identifierField = "identifier";


  static public final String refField = "ref";
  static public final String ref0Field = "ref0";
  static public final String ref1Field = "ref1";
  static public final String ref2Field = "ref2";
  static public final String ref3Field = "ref3";
  static public final String ref4Field = "ref4";

  static public final String nationField = "nation";

  static public final String geoipField = "geoip";

  static private final String[] refFieldsArray = {refField, ref0Field, ref1Field, ref2Field, ref3Field, ref4Field};
  static public final List<String> refFields = Arrays.asList(refFieldsArray);


  /**
   * 来源处理规则函数
   *
   * @param s 来源渠道
   * @return
   */
  public static String reslove(String s) {
    if (s.startsWith("xafrom=")) {
      int num = 0, num2i = 10000, num4i = 10000, i;
      for (i = 0; i < s.length(); i++) {
        if (s.charAt(i) == ';') {
          num++;
          if (num == 2) {
            num2i = i;
            break;
          }
        }
      }
      if (num2i + 1 < s.length()) return s.substring(num2i + 1);
      else return null;
    } else if (s.startsWith("sgfrom=")) {
//            return s.substring(7);
      if (s.length() > 7) return s.substring(7);
      else return null;
    } else return s;
  }

  /**
   * 所有的属性更新方法都在这里 3类型 valueType 和 3 更新方法 updateFunction
   * <p/>
   * 只有系统中注册了的用户属性才能更新。
   * 如果输入的是一个没有注册的属性，那么对应的UserProp为空。
   *
   * @param tempUser 用户对象
   * @param userProp 用户属性描述信息。如果不存在对应的属性，那么userProp为空。
   * @param key      更新的属性
   * @param value    更新的值对象
   * @return 更新了user的属性返回true，没有更新返回false
   */
  static public boolean updatePropertyIntoUser(String project, User tempUser, UserProp userProp, String key,
                                               Object value) {

    try {
      if (userProp == null) return false;
      //如果被命中，说明该属性为registerField，first_pay_time，last_login_time和ref*中的一个且之前已经有记录，就不更新user的属性。
      if (UserPropertyBitmaps.getInstance().isPropertyHit(project, tempUser.getSeqUid(), key))
        return false;
      //未命中的属性进行user的更新
      switch (userProp.getPropType()) {
        case sql_datetime:
          Long longValue = (Long) value;
          switch (userProp.getPropFunc()) {
            case once:
              if (!tempUser.containsKey(key)) tempUser.updateProperty(key, longValue);
              break;
            case cover:
              tempUser.updateProperty(key, longValue);
              break;
          }
          break;
        case sql_bigint:
          //int integer =(int)value;
          Integer integer = Integer.parseInt(value.toString());
          switch (userProp.getPropFunc()) {
            case once:
              break;
            case cover:
              tempUser.updateProperty((String) key, integer);
              break;
            case inc:
              tempUser.incProperty((String) key, integer);
              break;
          }
          break;
        case sql_string:
          String string = (String) value;
          if (string.trim().length() == 0)
            return false;
          switch (userProp.getPropFunc()) {
            case once:
              //ref字段的特殊判断，将翻译来源字段内容
              //如果，不合法，即为空，跳过
              //ref字段的处理在logparse里面实现
//                            if(key.equals("ref")){
//                                string = User.reslove(string);
//                                if(string == null) break;
//                            }
              //如果属性存在不更新
              if (!tempUser.containsKey(key)) tempUser.updateProperty(key, string);
              break;
            case cover:
              tempUser.updateProperty(key, string);
              break;
            case inc:
              break;
          }
          break;
      }
      //记录bitmap
      UserPropertyBitmaps.getInstance().markPropertyHit(project, tempUser.getSeqUid(), key);
    } catch (Exception e) {
      //可能属性转换失败，直接抛弃
      LOG.warn(project + "|" + key + "|" + value + "|" + userProp.getPropType() + "|" + e.getMessage(), e);
    }
    return true;
  }

  public long getSeqUid() {
    return seqUid;
  }


  public long getSamplingSeqUid() {
    return samplingSeqUid;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(seqUid);
    stringBuilder.append("\t");
    stringBuilder.append(JSONObject.fromObject(property).toString());
    return stringBuilder.toString();
  }

}
