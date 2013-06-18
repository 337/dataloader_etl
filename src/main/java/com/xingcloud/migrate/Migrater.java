package com.xingcloud.migrate;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.xingcloud.dataloader.driver.MongodbDriver;
import com.xingcloud.dataloader.lib.TimeIndex;
import com.xingcloud.util.Log4jProperties;
import com.xingcloud.util.manager.MailManager;
import com.xingcloud.util.manager.NetManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Migrater {
    public static final Log LOG = LogFactory.getLog(Migrater.class);
    private static final char separator = File.separatorChar;


    public static void main(String[] args) throws Exception {


        Log4jProperties.init();
        long t1 = System.currentTimeMillis();

        String type = args[0];


        if(!type.equals("fix") && ! type.equals("normal")) throw new Exception();

        String date;
        if (args != null && args.length > 1 && args[1] != null) {
            date = args[1];
        }
        else{
            date = TimeIndex.getDate();
        }

        int index;
        if (args != null && args.length > 2 && args[2] != null) {
            index = Integer.valueOf(args[2]);
        }
        else{
            index = TimeIndex.getTimeIndex();
        }

        if(type.equals("fix")){
            fixAllCopy(date,index);
        }
        else if(type.equals("normal")){
            copyFile("site_data", date, index);
            copyFile("store_log", date, index);
        }

    }

    private static void fixAllCopy(String date,int index) {
        fixCopy("site_data", date, index);
        fixCopy("store_log", date, index);

    }

    private static void fixCopy(String type,String date, int index) {
        DBObject query=new BasicDBObject();
        String publicIp = NetManager.getPublicIp();
        String siteIp = NetManager.getSiteLocalIp();
        query.put("public_ip", publicIp);
        query.put("inet_ip", siteIp);
        query.put("date", date);
        query.put("type", type);
        query.put("stats", "finish");

        int[] all=new int[290];
        DBCursor cursor = MongodbDriver.getInstanceDB().getCollection("copy").find(query);
        while(cursor.hasNext()){
            int i = (Integer) cursor.next().get("index");
            all[i]=1;
        }
         for(int j=0;j<=287 && j<index;j++){
            if(all[j]==0) {
                copyFile(type,date,j);
                LOG.info("fix "+type+date+j);
            }
         }
    }

    static  private void copyFile(String type,String date,int index){
        try{
            long t1=System.currentTimeMillis();
            String srcRoot = "/data/htdocs/elex_analytics";
            String distRoot = "/data/nfs_data";
            String pattern = getPatternFromDate(date);
            int min5Number = index;

            String currentLogFilePath = null;
            File check = null;

            Map<String, String> fileMap = new HashMap<String, String>();

            File appidDir = new File(srcRoot + separator + type);
            File[] files = appidDir.listFiles();

            String appid = null;
            for (File file : files) {
                if (!file.isDirectory()) {
                    continue;
                }
                appid = file.getName();
                currentLogFilePath = file.getAbsolutePath() + separator + pattern
                        + separator + min5Number + ".log";
                check = new File(currentLogFilePath);
                if (!check.exists()) {
                    continue;
                }
                fileMap.put(currentLogFilePath, appid);
                //System.out.println("[SHOULD-COPY]: " + currentLogFilePath);
            }
            ExecutorService service = Executors.newFixedThreadPool(10,
                    new CopyThreadFactory());

            File src = null;
            File dist = null;
            if (fileMap.isEmpty()) {
                System.exit(0);
            }
            LOG.info("begin "+type+" "+date+" "+index);
            for (Entry<String, String> entry : fileMap.entrySet()) {
                try {
                    src = new File(entry.getKey());
                    dist = new File(distRoot + separator + type + separator
                            + entry.getValue() + separator + pattern + separator
                            + src.getName());
                    service.execute(new FileCopier(src, dist, false));
                } catch (Exception e) {
                    e.printStackTrace();
                    continue;
                }
            }
            service.shutdown();
            boolean  finish = service.awaitTermination(10, TimeUnit.MINUTES);
            long t2 = System.currentTimeMillis();
            LOG.info("end : " + (t2 - t1) / 1000 + " seconds.");

            String publicIp = NetManager.getPublicIp();
            String siteIp = NetManager.getSiteLocalIp();
            DBObject finishObject=new BasicDBObject();
            finishObject.put("public_ip", publicIp);
            finishObject.put("inet_ip", siteIp);
            finishObject.put("date", date);
            finishObject.put("index", index);
            finishObject.put("type", type);
            if(finish) finishObject.put("stats", "finish");
            else finishObject.put("stats", "time out");

            MongodbDriver.getInstanceDB().getCollection("copy").insert(finishObject);
            if(!finish){
                MailManager.getInstance().sendEmail(publicIp+"copy to long more than 10 min","info:"+type+" "+date+" "+index);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }




    private static String getPatternFromDate(String date) {
        return date.substring(0,4)+File.separator+date.substring(4,6)+File.separator+"ea_data_"+date.substring(6,8);
    }
}
