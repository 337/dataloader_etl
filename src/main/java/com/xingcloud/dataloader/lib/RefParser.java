package com.xingcloud.dataloader.lib;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: liuxiong
 * Date: 13-11-19
 * Time: 上午11:30
 */
abstract class AbstractRefParser {

  static final String REF = User.refField;
  static final String REF0 = User.ref0Field;
  static final String REF1 = User.ref1Field;
  static final String REF2 = User.ref2Field;
  static final String REF3 = User.ref3Field;
  static final String REF4 = User.ref4Field;
  static final String[] REF_FIELDS = {REF0, REF1, REF2, REF3, REF4};

  static final String UTM_SOURCE = "utm_source=";
  static final String UTM_CONTENT = "utm_content=";
  static final String XA_FROM = "xafrom=";
  static final String SG_FROM = "sgfrom=";
  static final String FACEBOOK_AGE_REF_START = "{";

  static final int REF_COUNT = REF_FIELDS.length;
  static final int RAW_REF_COLON_SPLIT_INDEX = 2;

  static final ObjectMapper objectMapper = new ObjectMapper();

  abstract Map<String, String> parse(final String content);

}


class DefaultRefParser extends AbstractRefParser {

  @Override
  Map<String, String> parse(final String content) {
    Map<String, String> refMap = new HashMap<String, String>();
    if (content == null || content.trim().length() == 0) {
      return refMap;
    }

    refMap.put(REF, content.trim());
    return refMap;
  }

}


class XAFromRefParser extends AbstractRefParser {

  @Override
  Map<String, String> parse(final String content) {
    Map<String, String> refMap = new HashMap<String, String>();

    if (content == null || content.trim().length() == 0) {
      return refMap;
    }

    final String trimmedContent = content.trim();
    int lastStarIndex = trimmedContent.lastIndexOf("*");
    String[] segments = null;
    if (lastStarIndex == -1) {
      segments = trimmedContent.split(";");
    } else {
      segments = trimmedContent.substring(lastStarIndex + 1).split(";");
    }

    if (segments.length <= RAW_REF_COLON_SPLIT_INDEX) {
      if (trimmedContent.startsWith(UTM_SOURCE)) {
        //处理这种情况：utm_source=tapjoy
        UTMSourceRefParser utmSourceParser = new UTMSourceRefParser();
        refMap.putAll(utmSourceParser.parse(trimmedContent.substring(UTM_SOURCE.length())));
      } else {
        refMap.put(REF, trimmedContent);
      }
    }

    for (int i = RAW_REF_COLON_SPLIT_INDEX; i < segments.length; i++) {
      if (i < RAW_REF_COLON_SPLIT_INDEX + REF_COUNT - 1) {
        if (segments[i].trim().length() > 0) {
          refMap.put(REF_FIELDS[i - RAW_REF_COLON_SPLIT_INDEX], segments[i].trim());
        }
      } else {
        String lastRef = refMap.get(REF_FIELDS[REF_COUNT - 1]);
        if (lastRef == null)
          refMap.put(REF_FIELDS[REF_COUNT - 1], segments[i].trim());
        else
          refMap.put(REF_FIELDS[REF_COUNT - 1], lastRef + ";" + segments[i].trim());
      }
    }

    String ref4 = refMap.get(REF4);
    if (ref4 != null) {
      if (ref4.trim().length() == 0 || ref4.trim().replaceAll(";", "").length() == 0)
        refMap.remove(REF4);
    }

    return refMap;
  }

}


class SGFromRefParser extends AbstractRefParser {

  @Override
  Map<String, String> parse(final String content) {
    DefaultRefParser defaultRefParser = new DefaultRefParser();
    return defaultRefParser.parse(content);
  }

}


class UTMSourceRefParser extends AbstractRefParser {

  @Override
  Map<String, String> parse(final String content) {
    Map<String, String> refMap = new HashMap<String, String>();
    if (content == null || content.trim().length() == 0) {
      return refMap;
    }

    refMap.put(REF0, content.trim());
    return refMap;
  }

}


class UTMContentRefParser extends AbstractRefParser {

  @Override
  Map<String, String> parse(final String content) {
    XAFromRefParser xaFromParser = new XAFromRefParser();
    return xaFromParser.parse(content);
  }

}


class FacebookRefParser extends AbstractRefParser {

  @Override
  Map<String, String> parse(final String content) {
    Map<String, String> refMap = new HashMap<String, String>();
    if (content == null || content.trim().length() == 0) {
      return refMap;
    }

    final String trimmedContent = content.trim();
    try {
      Map facebookRefMap = objectMapper.readValue(trimmedContent, Map.class);
      if(facebookRefMap.containsKey("app") && facebookRefMap.containsKey("t")) {
        refMap.put(REF0, "f");
      } else {
        refMap.put(REF, trimmedContent);
      }
    } catch (IOException e) {
      refMap.put(REF, trimmedContent);
    }

    return refMap;
  }

}


public class RefParser {


  // 分析ref字段.
  // 以xafrom=开头，且xafrom=之后有值, 则放入ref0-ref4的5个属性;
  // 如果以sgfrom=开头，则更新到ref属性里;
  // xafrom的处理，如果存在"*"号, 取"*"号之后的内容中第二个";"号之后的内容;
  // 没有"*"号，就取第二个";"号的以后的内容


  private static final Map<String, AbstractRefParser> prefixParserMap;

  static {
    prefixParserMap = new HashMap<String, AbstractRefParser>();
    prefixParserMap.put(AbstractRefParser.XA_FROM, new XAFromRefParser());
    prefixParserMap.put(AbstractRefParser.SG_FROM, new SGFromRefParser());
    prefixParserMap.put(AbstractRefParser.FACEBOOK_AGE_REF_START, new FacebookRefParser());
    prefixParserMap.put(AbstractRefParser.UTM_SOURCE, new UTMSourceRefParser());
    prefixParserMap.put(AbstractRefParser.UTM_CONTENT, new UTMContentRefParser());
  }


  public static Map<String, String> parse(final String ref) {
    final String refContent = ref.trim();
    for (String prefix: prefixParserMap.keySet()) {
      if (refContent.startsWith(prefix)) {
        String content = null;
        if (prefix.equals(AbstractRefParser.FACEBOOK_AGE_REF_START)) {
          content = refContent;
        } else {
          content = refContent.substring(prefix.length());
        }
        return prefixParserMap.get(prefix).parse(content);
      }
    }

    DefaultRefParser defaultParser = new DefaultRefParser();
    return defaultParser.parse(refContent);
  }

}
