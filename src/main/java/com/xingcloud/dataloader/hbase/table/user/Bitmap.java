package com.xingcloud.dataloader.hbase.table.user;

import java.util.Arrays;
import java.util.Random;

/**
 * Author: mulisen
 * Date:   10/31/12
 */
public class Bitmap {


  private byte[] bits = new byte[1024];
  private static final int[] masks = new int[]{1, 2, 4, 8, 16, 32, 64, 128};
  private static final int[] ormasks = new int[]{0xff - 1, 0xff - 2, 0xff - 4, 0xff - 8, 0xff - 16, 0xff - 32, 0xff - 64, 0xff - 128};

  private long MAX_LENGTH = 10 * 1024 * 1024;// 10M bytes ,80M bits

  private long lower = -1l; //a reference value, decide if expand to lower or expand to upper

  public boolean get(long id) {
    if (lower < 0)
      return false;
    int offset = (int) ((id - lower) >> 3);
    return !(offset >= bits.length || offset < 0) && (bits[offset] & masks[((int) (id & 7))]) != 0;
  }

  public void set(long id, boolean is) {
    if (lower < 0)
      lower = id;    //init  lower
    if (id >= lower && id < lower + (bits.length << 3)) {
      int offset = (int) ((id - lower) >> 3);
      if (is) {
        bits[offset] = (byte) (bits[offset] | masks[((int) (id & 7))]);
        offset = 0;
      } else {
        bits[offset] = (byte) (bits[offset] & ormasks[((int) (id & 7))]);
      }
    } else if (id < lower) {
      //lower expand
      long newlower = id - (id & 7);

      int newlength = (int) ((lower - newlower) >> 3) + bits.length;
      if (newlength > MAX_LENGTH) {
//        System.out.println("bitmap overflow.newLength is:"+newlength+" id:"+id+"lower is "+lower);
        return;
      }
      byte newbits[] = new byte[newlength];
      System.arraycopy(bits, 0, newbits, newlength - bits.length, bits.length);
      //System.out.println(""+bits.length+" bytes copied.");
      if (is) {
        newbits[0] = (byte) (newbits[0] | masks[((int) (id & 7))]);
      } else {
        newbits[0] = (byte) (newbits[0] & ormasks[((int) (id & 7))]);
      }
      bits = newbits;
      lower = newlower;
    } else if (id >= lower + (bits.length << 3)) {
      //upper expand
      int newlength = (int) (((id - lower) >> 3) + 1);
      newlength = newlength > bits.length * 2 ? newlength : bits.length * 2 > MAX_LENGTH ? newlength: bits.length * 2;
      if (newlength > MAX_LENGTH) {
//        System.out.println("bitmap overflow.newLength is:"+newlength);
        return;
      }
      byte newbits[] = new byte[newlength];
      System.arraycopy(bits, 0, newbits, 0, bits.length);
      //System.out.println(""+bits.length+" bytes copied.");
      bits = newbits;
      int offset = (int) ((id - lower) >> 3);
      if (is) {
        bits[offset] = (byte) (bits[offset] | masks[((int) (id & 7))]);
        offset = 0;
      } else {
        bits[offset] = (byte) (bits[offset] & ormasks[((int) (id & 7))]);
      }
    }

  }

  public void reset() {
    Arrays.fill(bits, (byte) 0);
  }
}
