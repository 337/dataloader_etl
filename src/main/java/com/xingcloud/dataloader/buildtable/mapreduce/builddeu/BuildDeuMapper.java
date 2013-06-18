package com.xingcloud.dataloader.buildtable.mapreduce.builddeu;

import com.xingcloud.dataloader.hbase.table.EventMetaTable;
import com.xingcloud.dataloader.lib.Event;
import com.xingcloud.dataloader.lib.LocalPath;
import com.xingcloud.dataloader.lib.LogParser;
import com.xingcloud.dataloader.lib.User;
import com.xingcloud.util.Base64Util;
import com.xingcloud.util.Common;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Author: qiujiawei
 * Date:   12-4-26
 */
public class BuildDeuMapper extends Mapper<LongWritable,Text,Text,Text>{
    private LogParser dataLogParser=null;
    private LogParser storeLogParser=null;
    private Random random=new Random();
    private Text writeKey=new Text();
    private Text writeValue=new Text();
    private String project;
    private EventMetaTable eventMetaTable;

    @Override
    public void setup(Context context){
        project = context.getConfiguration().get("project");
        dataLogParser=new LogParser(LocalPath.SITE_DATA,null);
        storeLogParser=new LogParser(LocalPath.STORE_LOG,null);
        eventMetaTable=new EventMetaTable(project);
    }

    @Override
    public void map(LongWritable key, Text value,Context context)throws IOException,InterruptedException {
        int eventNum=0;
        int nullNum=0;


            String log=value.toString();
//            System.out.println(log);
            List<Event> eventList=null;
            if(log.contains("\t")){
                eventList=dataLogParser.parse(log);
            }
            else{
                eventList=storeLogParser.parse(log);
            }
            if(eventList!=null){
                for(Event event:eventList){
//                    if(!eventMetaTable.checkEvent(event)) continue;

                    if(event.getEvent()==null || event.getEvent().equals("update.") || event.getEvent().length()<2 ) continue;
                    String rowKey="";
                    if(User.isSpecialUid(event.getUid())){
                        rowKey=event.getDate()+event.getEvent()+ Base64Util.encodeBase64Uid(Integer.toString(random.nextInt()));
                    }
                    else{
                        rowKey=event.getDate()+event.getEvent()+Base64Util.encodeBase64Uid(event.getUid());
                    }
                    if(rowKey.length()>0){
    //                    writeKey.set(rowKey);
    //                    writeValue.set(event.getTimestamp() + "," + event.getValue());
                        context.write(new Text(rowKey),new Text(event.getTimestamp() + "," + event.getValue()));
                        eventNum++;
                    }
                    else{
                        nullNum++;
                    }
                }
            }

        context.getCounter("number","nullEvent").increment(nullNum);
        context.getCounter("number","eventNum").increment(eventNum);
    }


    @Override
    public void cleanup(Context context){
        eventMetaTable.flushMongodb();
    }

}
