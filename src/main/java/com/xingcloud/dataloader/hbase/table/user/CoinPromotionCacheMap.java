package com.xingcloud.dataloader.hbase.table.user;

import com.carrotsearch.hppc.LongIntOpenHashMap;
import com.xingcloud.dataloader.lib.Event;
import com.xingcloud.dataloader.lib.SeqUidCacheMapV2;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
 * Date: 5/25/13
 * Time: 1:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class CoinPromotionCacheMap {
    private static Log LOG= LogFactory.getLog(CoinPromotionCacheMap.class);
    private static CoinPromotionCacheMap instance;
    private Map<String,LongIntOpenHashMap> map;
    //private Map<String,Map<String,Connection>> cons;
    private CoinPromotionCacheMap(){
          map=new ConcurrentHashMap<String, LongIntOpenHashMap>();
          //cons= new HashMap<String, Map<String, Connection>>();
    }
    public static synchronized CoinPromotionCacheMap getInstance(){
        if(instance==null){
            instance=new CoinPromotionCacheMap();
        }
        return instance;
    }
    public int getVal(String project,long ssuid){
        LongIntOpenHashMap pidMap=map.get(project);
        if(pidMap==null){
            pidMap=LongIntOpenHashMap.newInstance();
            map.put(project,pidMap);
        }
        if(!pidMap.containsKey(ssuid)){
            put(project,ssuid,0);
        }
        int val=pidMap.get(ssuid);
        return val;
    }

    private Connection getNodeConn(String project, String nodeAddress,Map<String,Connection> cons) throws SQLException {
        long t1=System.currentTimeMillis();
        Connection con=cons.get(nodeAddress);
        if(con==null){
            con = MySql_16seqid.getInstance().getConnByNode(project, nodeAddress);
            cons.put(nodeAddress,con);
        }
        long t2=System.currentTimeMillis();
        LOG.info("get connection using "+(t2-t1)+" ms");
        return con;
    }

//    private String getDbName(String project) {
//        return "fix_"+project;
//        //return null;  //To change body of created methods use File | Settings | File Templates.
//    }

    public void put(String project, long ssuid, int val) {
        LongIntOpenHashMap pidMap=map.get(project);
        if(pidMap==null){
            pidMap=LongIntOpenHashMap.newInstance();
            map.put(project,pidMap);
        }
        pidMap.put(ssuid,val);
    }
    public void getCoinValues(String project, List<Event> eventList,Map<String, Connection > cons) {
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
                long seqUid= SeqUidCacheMapV2.getInstance().getUidCache(project,uid);
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
                        sql="select * from coin_promotion where uid in ("+samplingSeqUid;
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
    private static class MysqlRun implements Runnable{
        private  String node2;
        private long number;
        private String node1;
        public MysqlRun(long number,String node1,String node2){
            this.number=number;
            this.node1=node1;
            this.node2=node2;
        }
        public void run(){
            LOG.info("task number : "+number);
            long t1=System.currentTimeMillis();
            Connection con1=null;
            Connection con2=null;
            PreparedStatement statement=null;
            PreparedStatement sta2=null;
            ResultSet rs=null;
            try{
                con1=MySql_16seqid.getInstance().getConnByNode("age",node1);

                String query="select * from coin_buy limit 1000";
                String query2="select * from coin_promotion where uid in (390849835678)";
                LOG.info("get connection 1 success");
                statement=con1.prepareStatement(query);
                rs=statement.executeQuery();
                while(rs.next()){
                    //LOG.info(rs.getLong(1));
                }
                rs.close();
                con2=MySql_16seqid.getInstance().getConnByNode("age",node2);
                LOG.info("get connection 2 success");
                sta2=con2.prepareStatement(query2);
                rs=sta2.executeQuery();
                while(rs.next()){
                    LOG.info(rs.getLong(1));
                }
                LOG.info("executeQuery success");
                long t2=System.currentTimeMillis();
                LOG.info("task "+number+ "using "+(t2-t1)+ "ms");
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                try {
                    if(rs!=null)rs.close();
                    if(statement!=null)statement.close();
                    if(sta2!=null)sta2.close();
                    if(con1!=null)con1.close();
                    if(con2!=null)con2.close();
                } catch (SQLException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }

            }
        }
    }
    public static void main(String args[]){
        long taskNum=Long.parseLong(args[0]);
        String node1=args[1];
        String node2=args[2];
        ExecutorService exec= Executors.newFixedThreadPool(1);
        for (int i = 0; i < taskNum; i++) {
            exec.submit(new MysqlRun(i,node1,node2));
        }
        exec.shutdown();
        try {
            exec.awaitTermination(3600, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

}
