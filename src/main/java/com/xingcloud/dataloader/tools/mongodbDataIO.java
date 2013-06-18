package com.xingcloud.dataloader.tools;

import com.mongodb.*;
import com.mongodb.util.JSON;

import java.io.*;

/**
 * 用于mongodb的数据迁移
 * usage：mongodb_host mongodb_port 本地目录 类型（导入/导出）
 * Author: qiujiawei
 * Date:   12-7-17
 */
public class mongodbDataIO {
    static public void main(String[] args){
        try{
            String host=args[0];
            String port=args[1];

            String dir=args[2];

            if(args[3].equals("export")){
                exportM(host, port, dir);
            }
            else if(args[3].equals("import")){
                importM(host,port,dir);
            }


        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
    static public void exportM(String host,String port,String dir)throws Exception{
        Mongo mongo =new Mongo(host,Integer.parseInt(port));
        DB db = mongo.getDB("xingyun_analytics");

        for(String name:db.getCollectionNames()    ){
            DBCursor dbCursor=db.getCollection(name).find(new BasicDBObject("date","2012-07-13"));
            File file=new File(dir+"/"+name);
            DataOutputStream dataOutputStream= new DataOutputStream( new FileOutputStream(file));
            while(dbCursor.hasNext()){
                DBObject dbObject=dbCursor.next();
                dbObject.removeField("_id");
                dataOutputStream.write((dbObject.toString()+"\n").getBytes());
            }
            dataOutputStream.close();
        }
    }
    static public void importM(String host,String port,String dir)throws Exception{
        Mongo mongo =new Mongo(host,Integer.parseInt(port));
        DB db = mongo.getDB("xingyun_analytics");

        for(String name:db.getCollectionNames()    ){
            DBCollection collection=db.getCollection(name);
            File file=new File(dir+"/"+name);
            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line=null;
            while((line=bufferedReader.readLine())!=null){
                BasicDBObject object =(BasicDBObject) JSON.parse(line);
                collection.insert((BasicDBObject)object);
            }
            bufferedReader.close();
        }
    }
}
