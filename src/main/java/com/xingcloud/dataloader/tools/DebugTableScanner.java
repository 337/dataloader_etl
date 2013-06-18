package com.xingcloud.dataloader.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.NavigableMap;

/**
 * Author: qiujiawei
 * Date:   12-7-30
 */
public class DebugTableScanner {
     //check table
    public static void main(String[] args){
        String tableName=args[0];
        String startRowKey=null;
        if(args.length>1)startRowKey=args[1];
        String endRowKey=null;
        if(args.length>2) endRowKey=args[2];
        HTable hTable=null;
        try {

            Configuration conf = HBaseConfiguration.create();
            hTable=new HTable(conf,tableName.getBytes());

            int total=0;
            Scan scan = new Scan();
            if(startRowKey!=null)scan.setStartRow(startRowKey.getBytes());
            if(endRowKey!=null)scan.setStartRow(endRowKey.getBytes());

            scan.setMaxVersions();
            ResultScanner scanner=hTable.getScanner(scan);
            Result result=new Result();
            while((result=scanner.next())!=null){
                printRe(result);
            }


        } catch (Exception e) {
            e.printStackTrace();  //e:
        }
    }
    private static void printRe(Result re){
//        System.out.print("row "+new String(re.getRow())+" ");
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = re.getMap();
        for(NavigableMap.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> kv:map.entrySet())
        {
//            System.out.print(" f:"+new String(kv.getKey())+" q num:"+kv.getValue().size());
            int qulifier=0;
            for(NavigableMap.Entry<byte[], NavigableMap<Long, byte[]>> kv2:kv.getValue().entrySet()){
//                System.out.print(" q:"+new String(kv2.getKey()));
//                System.out.print(" [ ");
                for(NavigableMap.Entry<Long, byte[]>kv3 :kv2.getValue().entrySet()){
//                    System.out.print(" ts:"+kv3.getKey());
                    if(kv3.getValue().length==8){
                        System.out.println(new String(re.getRow())+" "+kv3.getKey()+" "+Bytes.toLong(kv3.getValue()));
                    }
                }
//                System.out.print(" ] ");
                qulifier++;
                if(qulifier>20) break;
            }
        }
    }
}
