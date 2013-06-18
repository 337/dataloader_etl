package com.xingcloud.dataloader.hbase.table.user;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.xingcloud.dataloader.lib.Event;
import com.xingcloud.dataloader.lib.SeqUidCacheMap;
import com.xingcloud.mysql.MySql_fixseqid;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.beans.Statement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 5/24/13
 * Time: 8:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class CoinBuyCacheMap {
    private static Log LOG = LogFactory.getLog(CoinBuyCacheMap.class);
    private static CoinBuyCacheMap instance;
    private Map<String,LongIntOpenHashMap> map;
    //private Map<String,Map<String,Connection>> cons;
    public synchronized static CoinBuyCacheMap getInstance(){
        if(instance==null){
            instance=new CoinBuyCacheMap();
        }
        return instance;
    }
    private CoinBuyCacheMap() {
        map = new ConcurrentHashMap<String, LongIntOpenHashMap>();
        //cons= new HashMap<String, Map<String, Connection>>();
    }
    public int getVal(String project,Long ssuid){
        LongIntOpenHashMap pidMap=map.get(project);
        if(pidMap==null){
            pidMap=LongIntOpenHashMap.newInstance();
            map.put(project,pidMap);
        }
        if(!pidMap.containsKey(ssuid)){
            /*
            String dbName=getDbName(project);
            String node=UidMappingUtil.getInstance().hash(ssuid);
            try {
                Connection con=getNodeConn(dbName,node);
                //Connection con= MySql_fixseqid.getInstance().getConnByUid(dbName,seqUid);
                java.sql.Statement statement=con.createStatement();
                long samplingSeqUid=UidMappingUtil.getInstance().decorateWithSampling(ssuid);
                String queryStat="select val from coin_buy where uid="+samplingSeqUid;
                ResultSet rs= statement.executeQuery(queryStat);
                if(rs==null){
                    pidMap.put(ssuid,0);
                }
                else {
                    while(rs.next()){
                        int val=rs.getInt(1);
                        pidMap.put(ssuid,val);
                        break;
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            */
            put(project,ssuid,0);
        }
        int val=pidMap.get(ssuid);
        return val;
        //return 0;
    }
    private Connection getNodeConn(String project, String nodeAddress,Map<String,Connection> cons) throws SQLException {
        long t1=System.currentTimeMillis();
        Connection con=cons.get(nodeAddress);
        if(con==null){
             con = MySql_fixseqid.getInstance().getConnByNode(project, nodeAddress);
             cons.put(nodeAddress,con);
        }
        long t2=System.currentTimeMillis();
        LOG.info("get connection using "+(t2-t1)+" ms");
        return con;
    }

    private String getDbName(String project) {
        return "fix_"+project;
        //return null;  //To change body of created methods use File | Settings | File Templates.
    }

    public void put(String project,long ssuid,int val){
        LongIntOpenHashMap pidMap=map.get(project);
        if(pidMap==null){
            pidMap=LongIntOpenHashMap.newInstance();
            map.put(project,pidMap);
        }
        pidMap.put(ssuid,val);
    }

    public void getCoinValues(String project, List<Event> eventList,Map<String,Connection> cons) {
         Map<String,String> sqlsMap=getSqlsFromEvents(project,eventList);
         excute(project,sqlsMap,cons);
    }

    private void excute(String project,Map<String, String> sqlsMap,Map<String,Connection> cons) {
         //String dbName=getDbName(project);
         for(Map.Entry<String,String> entry: sqlsMap.entrySet()){
             String node=entry.getKey();
             Connection conn=null;
             PreparedStatement statement=null;
             ResultSet rs=null;
             try {
                 conn=getNodeConn(project,node,cons);

                 String sql=entry.getValue();
                 LOG.info(sql);
                 statement = conn.prepareStatement(sql);
                 LOG.info("create statement");

                  rs=statement.executeQuery();
                 LOG.info("get result");
                 while(rs.next()){
                     long samplingSeqUid=rs.getLong(1);
                     //long seqUid=samplingSeqUid & (0xFFFFFFFF);
                     int  value=rs.getInt(2);
                     put(project,samplingSeqUid,value);
                 }
             } catch (SQLException e) {
                 e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
             }finally {
                 try {
                 if(rs!=null)rs.close();
                 if(statement!=null)
                     statement.close();
                 //if(conn!=null)conn.close();
                 } catch (SQLException e) {
                     e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                 }
             }
         }

    }
    //

    private Map<String, String> getSqlsFromEvents(String project,List<Event> eventList) {
        Map<String,String> sqlsMap=new HashMap<String, String>();
        List<Long> seqUidList=new ArrayList<Long>();
        Map<String,String> result=new HashMap<String, String>();
        for(Event event: eventList){
            String uid=event.getUid();
            try {
                long seqUid= SeqUidCacheMap.getInstance().getUidCache(project,uid);
                LongIntOpenHashMap projectMap=map.get(project);
                if(projectMap==null){
                    projectMap=new LongIntOpenHashMap();
                    map.put(project,projectMap);
                }
                long samplingSeqUid=UidMappingUtil.getInstance().decorateWithMD5(seqUid);
                if(!projectMap.containsKey(samplingSeqUid)){
                    seqUidList.add(seqUid);

                    String node=UidMappingUtil.getInstance().hash(seqUid);
                    String sql=sqlsMap.get(node);
                    if(sql==null){
                        sql="select * from coin_buy where uid in ("+samplingSeqUid;
                        sqlsMap.put(node,sql);
                    }
                    else{
                        sql+=","+samplingSeqUid;
                        sqlsMap.put(node,sql);
                    }

                }

            } catch (Exception e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }

            for(Map.Entry<String,String> entry: sqlsMap.entrySet()){
                String node=entry.getKey();
                String sql=entry.getValue();
                sql+=")";
                result.put(node,sql);
            }
        }
        return result;
    }

}
