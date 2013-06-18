package com.xingcloud.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * User: IvyTang
 * Date: 12-10-26
 * Time: 下午3:06
 */
public class RandSpeUidMd5 {

    static public final int max = Integer.MAX_VALUE;


    static ThreadLocal<MessageDigest> md5s = new ThreadLocal<MessageDigest>() {
        @Override
        protected MessageDigest initialValue() {
            try {
                return MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();  //e:
            }
            return null;
        }
    };

    static MessageDigest getMD5() {
        return md5s.get();
    }


//	static public long max = Long.MAX_VALUE;


    public static int md5(long seqUid) {
        try {
            byte[] bytes = toBytes(seqUid);
            MessageDigest digest = md5s.get();
            digest.reset();
            long hash = Base64Util.toLong(digest.digest(bytes), 4);
            hash = max & hash;
            return Integer.valueOf(String.valueOf(hash));
        } catch (Exception e) {
            e.printStackTrace();  //e:
        }
        return 0;
    }

    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }
}
