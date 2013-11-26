package com.xingcloud.dataloader.lib;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * User: liuxiong
 * Date: 13-11-19
 * Time: 上午11:15
 */
public class RefParserTest {

  private static final String REF = AbstractRefParser.REF;
  private static final String REF0 = AbstractRefParser.REF0;
  private static final String REF1 = AbstractRefParser.REF1;
  private static final String REF2 = AbstractRefParser.REF2;
  private static final String REF3 = AbstractRefParser.REF3;
  private static final String REF4 = AbstractRefParser.REF4;

  @Test
  public void testAnalyseRef() {
    String ref = "xafrom=n=C*k=*c=32069512676*s=www.oneonlinegames.com*br;ddt;g;c;Content-KT2;ddt50lp2;hello;world;;hello;";
    Map<String, String> refMap = RefParser.parse(ref);
    assertEquals(refMap.size(), 5);
    assertEquals(refMap.get(REF0), "g");
    assertEquals(refMap.get(REF1), "c");
    assertEquals(refMap.get(REF2), "Content-KT2");
    assertEquals(refMap.get(REF3), "ddt50lp2");
    assertEquals(refMap.get(REF4), "hello;world;;hello");

    ref = "xafrom=utm_source=tapjoy";
    refMap = RefParser.parse(ref);
    assertEquals(refMap.size(), 1);
    assertEquals(refMap.get(REF0), "tapjoy");

    ref = "utm_content=us;hayday@337;tapjoy;us;;";
    refMap = RefParser.parse(ref);
    assertEquals(refMap.size(), 2);
    assertEquals(refMap.get(REF0), "tapjoy");
    assertEquals(refMap.get(REF1), "us");

    ref = "utm_source=g";
    refMap = RefParser.parse(ref);
    assertEquals(refMap.size(), 1);
    assertEquals(refMap.get(REF0), "g");

    ref = "sgfrom=hello";
    refMap = RefParser.parse(ref);
    assertEquals(refMap.size(), 1);
    assertEquals(refMap.get(REF), "hello");

    ref = "{\"hello\":\"world\"}";
    refMap = RefParser.parse(ref);
    assertEquals(refMap.size(), 1);
    assertEquals(refMap.get(REF), ref);

    ref = "{\"hello\":\"world\", \"app\": \"app\", \"t\": \"t\"}";
    refMap = RefParser.parse(ref);
    assertEquals(refMap.size(), 1);
    assertEquals(refMap.get(REF0), "f");

  }

}
