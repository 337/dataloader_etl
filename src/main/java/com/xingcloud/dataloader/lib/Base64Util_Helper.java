package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.hbase.hash.HBaseHash;
import com.xingcloud.dataloader.hbase.hash.HBaseKeychain;
import com.xingcloud.xa.uidmapping.UidMappingUtil;

/**
 * User: IvyTang
 * Date: 13-6-14
 * Time: 下午5:01
 */
public class Base64Util_Helper {
  public static byte[] toBytes(long val) {
    byte[] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * Converts a byte array to a long value. Reverses
   * {@link #toBytes(long)}
   *
   * @param bytes array
   * @return the long value
   */
  public static long toLong(byte[] bytes) {
    return toLong(bytes, 0);
  }

  public static long toLong(byte[] bytes, int offset) {
    int length = 8;
    if (offset + length > bytes.length) {
      throw new IndexOutOfBoundsException("" + bytes + offset + length);
    }
    long l = 0;
    for (int i = offset; i < offset + length; i++) {
      l <<= 8;
      l ^= bytes[i] & 0xFF;
    }
    return l;
  }

  public static int toInteger(byte[] bytes) {
    return toInteger(bytes, 0);
  }

  public static int toInteger(byte[] bytes, int offset) {
    int length = 4;
    if (offset + length > bytes.length) {
      throw new IndexOutOfBoundsException("" + bytes + offset + length);
    }
    int value = 0;
    for (int i = offset; i < offset + length; i++) {
      value <<= 8;
      value ^= bytes[i] & 0xFF;
    }
    return value;

  }

  public static byte[] toBytes(int val) {
    byte[] b = new byte[4];
    for (int i = 3; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }



  public static byte[] charToByte(char c) {
    byte[] b = new byte[2];
    b[0] = (byte) ((c & 0xFF00) >> 8);
    b[1] = (byte) (c & 0xFF);
    return b;
  }

  public static char byteToChar(byte[] b) {
    char c = (char) (((b[0] & 0xFF) << 8) | (b[1] & 0xFF));
    return c;
  }

  public static void main(String[] args){
    for(HBaseHash hBaseHash:HBaseKeychain.getInstance().getConfigs()){
      System.out.println(hBaseHash.configs().keySet());

    }
  }
}
