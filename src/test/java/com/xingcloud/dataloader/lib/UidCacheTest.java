package com.xingcloud.dataloader.lib;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.xingcloud.util.HashFunctions;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * User: liuxiong
 * Date: 13-10-29
 * Time: 下午5:52
 */
public class UidCacheTest {

  Random random = new Random(System.currentTimeMillis());

  @Test
  public void testInitCacheFromNotExistFile() {
    String projectNotExist = "project_not_exist";
    SeqUidCacheMapV2.getInstance().initCache(projectNotExist);

    Map<String, LongIntOpenHashMap> map = SeqUidCacheMapV2.getInstance().getMap();
    assertNull(map.get(projectNotExist));
  }


  @Test
  public void testFlushAndInitCache() {
    String project = "test_project";
    LongIntOpenHashMap projectMap = new LongIntOpenHashMap();

    final int SIZE = random.nextInt(1000000) + 1000000;
    for (int i = 0; i < SIZE; i++) {
      projectMap.put(random.nextLong(), random.nextInt());
    }

    Map<String, LongIntOpenHashMap> map = SeqUidCacheMapV2.getInstance().getMap();
    map.put(project, projectMap);

    SeqUidCacheMapV2.getInstance().flushCacheToLocal(project);
    map.remove(project);
    SeqUidCacheMapV2.getInstance().initCache(project);

    assertEquals(map.get(project), projectMap);
  }


  public void testGetUid() {
    String project = "test_project";

    Map<String, LongIntOpenHashMap> map = SeqUidCacheMapV2.getInstance().getMap();
    LongIntOpenHashMap projectMap = new LongIntOpenHashMap();
    map.put(project, projectMap);

    long start = System.currentTimeMillis();

    for (int i = Integer.MAX_VALUE, count = 0; count < 10000; i--, count++) {
      try {
        String rawUid = String.valueOf(i);
        int innerUid = SeqUidCacheMapV2.getInstance().getUidCache(project, rawUid);
        assertEquals(projectMap.get(HashFunctions.md5(rawUid.getBytes())), innerUid);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    System.out.println(System.currentTimeMillis() - start);
  }
}
