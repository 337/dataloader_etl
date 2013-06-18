package com.xingcloud.dataloader.buildtable.buildmysqltable;

import com.xingcloud.util.Base64Util;
import com.xingcloud.util.Common;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Author: qiujiawei
 * Date:   12-6-7
 */
public class UserPropertyMapper extends Mapper<LongWritable,Text,Text,Text> {

    private String beginDate = null;
    private String endDate = null;
    private boolean judge = false;

    @Override
    public void setup(Context context){
        beginDate=context.getConfiguration().get("begin","no");
        endDate=context.getConfiguration().get("end","no");

        if(!beginDate.equals("no") && !endDate.equals("no"))  judge=true;
    }



    @Override
    public void map(LongWritable key, Text value,Context context)throws IOException,InterruptedException {
        try{
            String uid=null;
            String appid=null;
            String last_login_time=null;
            String temp=value.toString();
            for(String pair:temp.split("\t")){
                String[] kv=pair.split("=");
                if(kv.length==2){
                    if(kv[0].equals("uid")){
                        uid=kv[1];
                    }
                    else if(kv[0].equals("last_login_time")){
                        last_login_time=kv[1];
                    }
                }
                if(uid!=null && last_login_time!=null) break;
            }

            if(uid!=null){
                if(judge){
                    if(last_login_time!=null && last_login_time.compareTo(beginDate)>=0 && last_login_time.compareTo(endDate)<=0){
                        context.write(new Text(Base64Util.encodeBase64Uid(uid)),value);
                    }
                }
                else context.write(new Text(Base64Util.encodeBase64Uid(uid)),value);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}