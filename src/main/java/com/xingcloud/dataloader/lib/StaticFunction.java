package com.xingcloud.dataloader.lib;

/**
 * Author: qiujiawei
 * Date:   12-3-15
 */
public class StaticFunction {
    public static String ensureLength(String temp, int length){
        if(temp.length()>length)return temp.substring(0,length);
        else return temp;
    }
}
