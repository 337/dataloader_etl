package com.xingcloud.dataloader.buildtable.mapreduce.builddeu;

import com.xingcloud.dataloader.hbase.table.DeuTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Author: qiujiawei
 * Date:   12-4-26
 */

public class BuildDeuReducer extends TableReducer<Text,Text,ImmutableBytesWritable> {
    static String cfName="val";
    public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException {
        try{
            String k = key.toString();
            Put put = new Put(k.getBytes());
            for(Text value: values){
                String[] t=value.toString().split(",");
                long evnetTimestamp=Long.valueOf(t[0]);
                long eventValue=Long.valueOf(t[1]);
                put.add(DeuTable.columnFamily.getBytes(), DeuTable.columnFamily.getBytes(), evnetTimestamp,Bytes.toBytes(eventValue));
            }
            context.write(new ImmutableBytesWritable(k.getBytes()), put);
            context.getCounter("reduce","rowkey").increment(1);
        }
        catch (Exception e){
            context.getCounter("exception","reduce").increment(1);
        }
        context.getCounter("reduce","call").increment(1);
    }

}