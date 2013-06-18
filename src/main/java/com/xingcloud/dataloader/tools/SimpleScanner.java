package com.xingcloud.dataloader.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.NavigableMap;

/**
 * 查看某个hbase表的内容
 * usage： 表名 开始的行名 开始的列名（可以为空)
 * Author: qiujiawei
 * Date:   12-5-31
 */
public class SimpleScanner {
    //check table
    public static void main(String[] args){
        String tableName=args[0];
        String startRowKey=null;
        if(args.length>1)startRowKey=args[1];
        String startQulifier=null;
        if(args.length>2)startQulifier=args[2];
        HTable hTable=null;
        try {

            Configuration conf = HBaseConfiguration.create();
            hTable=new HTable(conf,tableName.getBytes());

            int total=0;
            Scan scan = new Scan();
            if(startRowKey!=null)scan.setStartRow(startRowKey.getBytes());

            scan.setMaxVersions();
            if(startQulifier!=null)scan.setFilter(new ColumnRangeFilter(startQulifier.getBytes(),true,startQulifier.getBytes(),true));
            ResultScanner scanner=hTable.getScanner(scan);
            Result result=new Result();
            while((result=scanner.next())!=null){
                printRe(result);
                total++;
                if(total>100) break;
            }


        } catch (Exception e) {
            e.printStackTrace();  //e:
        }
    }
    private static void printRe(Result re){
        System.out.print("row "+new String(re.getRow())+" ");
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = re.getMap();
        for(NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> kv:map.entrySet())
        {
            System.out.print(" f:"+new String(kv.getKey())+" q num:"+kv.getValue().size());
            int qulifier=0;
            for(NavigableMap.Entry<byte[], NavigableMap<Long, byte[]>> kv2:kv.getValue().entrySet()){
                System.out.print(" q:"+new String(kv2.getKey()));
                System.out.print(" [ ");
                for(NavigableMap.Entry<Long, byte[]>kv3 :kv2.getValue().entrySet()){
                    System.out.print(" ts:"+kv3.getKey());
                    if(kv3.getValue().length==8){
                        System.out.print(" value:"+Bytes.toLong(kv3.getValue()));
                    }
                }
                System.out.print(" ] ");
                qulifier++;
                if(qulifier>20) break;
            }
        }
        System.out.println();
    }
}
