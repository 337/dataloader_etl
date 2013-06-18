package com.xingcloud.dataloader.tools;

/**
 * 闰秒测试
 * Author: qiujiawei
 * Date:   12-7-2
 */
public class TestSleep {
    static public void main(String[] args){
        try{
            System.out.println("begin");
            Thread.sleep(1000);
            System.out.println("finish");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
