package com.xingcloud.dataloader.hbase.table.user;

import org.junit.Test;

import java.util.Random;

public class TestBitmap {

  @Test
  public void testBitmap() throws Exception {
    Bitmap bitmap = new Bitmap();
    Random rand = new Random();
    for (int i = 0; i < 10000000; i++) {
      long id = rand.nextInt(30000000);
      if (bitmap.get(id)) {
        System.err.println("error false!" + id);
      }
      bitmap.set(id, true);
      if (!bitmap.get(id)) {
        System.err.println("error true!" + id);
      }
      bitmap.set(id, false);
      if (bitmap.get(id)) {
        System.err.println("error false!" + id);
      }
    }
    byte[] results = new byte[10000];
    testSerial(bitmap, results);
    testSerial(bitmap, results);
    long t1 = System.currentTimeMillis();
    testSerial(bitmap, results);
    long t2 = System.currentTimeMillis();
    System.out.println(t2 - t1);
    for (int i = 0; i < results.length; i++) {
      byte result = results[i];
      if (result == 1 && !bitmap.get(i)) {
        System.err.println("error serial true!");
      } else if (result == 2 && bitmap.get(i)) {
        System.err.println("error serial false!");
      }
    }
    for (int i = 0; i < 1000; i++) {
      bitmap.set(i, true);
    }
  }

  private void testSerial(Bitmap bitmap, byte[] results) {
    Random rand = new Random();
    for (int i = 0; i < 10000000; i++) {
      long id = i;//rand.nextInt(results.length);
      boolean hit = true;//rand.nextBoolean();
      bitmap.set(id, hit);
      //bitmap.get(id);
        /*
        if(hit){
  				results[((int) id)] = 1;
  			}else{
  				results[((int) id)] = 2;//false
  			}
  			if(bitmap.get(id)!=hit){
  				System.err.println("error random !"+id);
  			}                        */
    }
  }

}
