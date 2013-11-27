package com.xingcloud.dataloader.lib;


import com.xingcloud.id.c.IdClient;
import com.xingcloud.id.pub.IdResult;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.util.Constants;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created with IntelliJ IDEA.
 * User: yangbo
 * Date: 6/6/13
 * Time: 4:15 PM
 * To change this template use File | Settings | File Templates.
 */
public class OrigIdSeqUidCacheMap {
    private static Log LOG = LogFactory.getLog(OrigIdSeqUidCacheMap.class);

    private static OrigIdSeqUidCacheMap instance;

    //private Map<String, LongIntOpenHashMap> map;
    private Map<String, UidSeqUidCache> map;
    Random random=new Random(1000000000l);

    /* Tmp for counting */
    private int missCount;
    private int missCount_govome;
    private int missCount_v9_v9;
    private int missCount_globososo;
    private int missCount_sof_dsk;
    private int missCount_sof_newgdp;
    private int missCount_i18n_status;
    private int missCount_sof_newhpnt;

    private int allCount;
    private int allCount_govome;
    private int allCount_v9_v9;
    private int allCount_globososo;
    private int allCount_sof_dsk;
    private int allCount_sof_newgdp;
    private int allCount_i18n_status;
    private int allCount_sof_newhpnt;

    private Map<String, Long> getUidTime;

    private Map<String, Map<String, Connection>> pidNodeConnections;


    public synchronized static OrigIdSeqUidCacheMap getInstance() {
        if (instance == null) {
            instance = new OrigIdSeqUidCacheMap();
        }

        return instance;
    }

    private OrigIdSeqUidCacheMap() {
        map = new ConcurrentHashMap<String, UidSeqUidCache>();
        getUidTime = new ConcurrentHashMap<String, Long>();
        pidNodeConnections = new HashMap<String, Map<String, Connection>>();
    }

    private void put(String pID, String RawUid, Long seqUid) {
        UidSeqUidCache cache = map.get(pID);
        if (cache == null) {
            cache = new UidSeqUidCache(10*Constants.CACHECAPACITY);
            LOG.info("First time init " + pID + " cache.");
            map.put(pID, cache);
        }
        cache.put(RawUid, seqUid);
    }

    private Long get(String pID, String RawUid) {
        if (!map.containsKey(pID)) {
            return new Long(0);
        }
        UidSeqUidCache cache = map.get(pID);
        if(!cache.containsKey(RawUid)){
            return new Long(0);
        }
        return cache.get(RawUid);
    }

    /**
     * 把uid cache flush到本地
     *
     * @throws java.io.IOException
     */
    public void flushCacheToLocal(String pID) throws IOException {
        long startTime = System.nanoTime();
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(Constants.UID_CACHE_LOCAL_FILE_PREFIX + pID));
            UidSeqUidCache cache = map.get(pID);
            //cache.entrySet();
            if (cache != null) {
                for(Map.Entry<String,Long> entry: cache.entrySet()) {

                        bw.write(entry.getKey() + "\t" + entry.getValue());
                        bw.newLine();
                }
                bw.flush();
                LOG.info("------UIDCACHE " + pID + " -------- flush uid cache of compled. Size:" + cache.size() +
                        "Taken: " + (System.nanoTime() - startTime) / 1.0e9 + "s.");
            } else {
                LOG.info("------UIDCACHE " + pID + " -------- uid cache is empty.");
            }
        } catch (IOException e) {
            LOG.error("------UIDCACHE" + pID + "-------- flush uid cache errors." + e.getMessage());
        } finally {
            if (bw != null)
                bw.close();
            // map.remove(pID);缓存flush到本地后，内存不清空
        }
    }

    /**
     * 从本地恢复uid cache ,如果map中有这个pid，则直接return。
     *
     * @throws IOException
     */
    public void initCache(String pID) throws IOException {
        if (map.get(pID) != null)
            return;
        long startTime = System.nanoTime();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(Constants.UID_CACHE_LOCAL_FILE_PREFIX + pID));
            String tempString = null;
            int counter = 0;
            while ((tempString = br.readLine()) != null) {
                int pos = tempString.indexOf("\t");
                if (pos < 0)
                    continue;
                put(pID, tempString.substring(0, pos), Long.parseLong(tempString.substring(pos + 1)));
                counter++;
            }
            LOG.info("------UIDCACHE " + pID + " -------- init cahce compled. Size:" + counter + " Taken: " + (System
                    .nanoTime() - startTime) / 1.0e9 + "s.");
        } catch (FileNotFoundException e) {
            LOG.info("------UIDCACHE " + pID + " -------- no uid cahce found,new pid Continue...");
        } catch (Exception e) {
            map.remove(pID);
            LOG.error("------UIDCACHE " + pID + " -------- parse uid cache " + Constants.UID_CACHE_LOCAL_FILE_PREFIX
                    + pID + " errors.Clear uid cache map" + e.getMessage());
        } finally {
            if (br != null)
                br.close();
        }
    }


    /**
     * 从缓存中拿到cache的uid
     *
     * @param project 项目名
     * @param rawUid  rawUid
     * @return
     * @throws Exception
     */
    public Long getUidCache(String project, String rawUid) throws Exception {
        long s = System.nanoTime();
        //long md5RawUid = HashFunctions.md5(rawUid.getBytes());
        Long seqUid = get(project, rawUid);
        if (seqUid == 0) {
            LOG.info("not hit "+rawUid);
            //seqUid= random.nextLong();
            //if(seqUid<0)seqUid=seqUid*(-1);
            IdResult idResult = IdClient.getInstance().getOrCreateId(project, rawUid);
            seqUid = idResult.getId();
            if (seqUid < 0) {
                //throw new Exception(project + " " + rawUid + " getCreatedId failed");
            }
            //保证每个在sequidcachemap里面的uid，是在热表/或者从冷表转到热表/或者2个表都不在（新增的）的uid；flush_fix就不需要在每次flush时候去check这个uid是否需要从冷表转。
            //本地缓存不在的uid；执行chechInHot和coldtohot
            /*
            if(project.equals("govome") || project.equals("globososo")){
                //do not need cold to host
            } else{
                if (!checkInHot(project, seqUid))
                    cold2Hot(project, UidMappingUtil.getInstance().decorateWithMD5(seqUid));
            }  */

            put(project, rawUid, seqUid);
            missCount++;


            if (project.equals("v9-v9"))
                missCount_v9_v9++;
            else if (project.equals("govome"))
                missCount_govome++;
            else if (project.equals("globososo"))
                missCount_globososo++;
            else if (project.equals("sof-dsk"))
                missCount_sof_dsk++;
            else if (project.equals("sof-newgdp"))
                missCount_sof_newgdp++;
            else if (project.equals("i18n-status"))
                missCount_i18n_status++;
            else if (project.equals("sof-newhpnt"))
                missCount_sof_newhpnt++;

        }

        addGetUidTime(project, System.nanoTime() - s);

        allCount++;


        if (project.equals("v9-v9"))
            allCount_v9_v9++;
        else if (project.equals("govome"))
            allCount_govome++;
        else if (project.equals("globososo"))
            allCount_globososo++;
        else if (project.equals("sof-dsk"))
            allCount_sof_dsk++;
        else if (project.equals("sof-newgdp"))
            allCount_sof_newgdp++;
        else if (project.equals("i18n-status"))
            allCount_i18n_status++;
        else if (project.equals("sof-newhpnt"))
            allCount_sof_newhpnt++;
        return seqUid;
    }


    private void addGetUidTime(String project, long timeUsed) {
        Long time = getUidTime.get(project);
        if (time == null)
            time = 0l;
        time += timeUsed;
        getUidTime.put(project, time);
    }


    public long oneReadTaskGetUidNanoTime(String project) {
        return getUidTime.get(project) == null ? 0 : getUidTime.get(project);
    }

    public void printStats() {
        long hitRatio = 0;
        if (allCount != 0) {
            hitRatio = 100 - missCount * 100 / allCount;
        }
        LOG.info("------UIDCACHE-------- 5min task cache allCount: " + allCount +
                " ;hitCount: " + (allCount - missCount) + " ;hitRatio: " + hitRatio + "%");

        long v9_v9_UidTime = getUidTime.get("v9-v9") == null ? 0 : getUidTime.get("v9-v9");
        long govome_UidTime = getUidTime.get("govome") == null ? 0 : getUidTime.get("govome");
        long globososo_UidTime = getUidTime.get("globososo") == null ? 0 : getUidTime.get("globososo");
        long sof_dsk_UidTime = getUidTime.get("sof-dsk") == null ? 0 : getUidTime.get("sof-dsk");
        long sof_newgdp_UidTime = getUidTime.get("v9-sof") == null ? 0 : getUidTime.get("v9-sof");
        long i18n_status_UidTime = getUidTime.get("i18n-status") == null ? 0 : getUidTime.get("i18n-status");
        long sof_newhpnt_UidTime = getUidTime.get("sof-newhpnt") == null ? 0 : getUidTime.get("sof-newhpnt");


        LOG.info("------UIDCACHE-------- getUid v9-v9:" + v9_v9_UidTime / 1000000 + "ms.govome:" +
                govome_UidTime / 1000000 + "ms.globososo:" + globososo_UidTime / 1000000 + "ms" +
                ".sof_dsk:" + sof_dsk_UidTime / 1000000 + "ms.v9-sof:" + sof_newgdp_UidTime / 1000000 + "ms" +
                ".i18n_statu:" + i18n_status_UidTime / 1000000 + "ms.sof_newhpnt:" + sof_newhpnt_UidTime / 1000000 + "ms.");
        LOG.info("------UIDCACHE-------- cache hit v9-v9: allCount" + allCount_v9_v9 + " hitCount:" +
                (allCount_v9_v9 - missCount_v9_v9));
        LOG.info("------UIDCACHE-------- cache hit govome: allCount: " + allCount_govome + " hitCount: " +
                (allCount_govome - missCount_govome));
        LOG.info("------UIDCACHE-------- cache hit globososo: allCount: " + allCount_globososo + " hitCount: " +
                (allCount_globososo - missCount_globososo));
        LOG.info("------UIDCACHE-------- cache hit sof_dsk: allCount: " + allCount_sof_dsk + " hitCount: " +
                (allCount_sof_dsk - missCount_sof_dsk));
        LOG.info("------UIDCACHE-------- cache hit v9-sof: allCount: " + allCount_sof_newgdp + " hitCount: " +
                (allCount_sof_newgdp - missCount_sof_newgdp));
        LOG.info("------UIDCACHE-------- cache hit i18n_status: allCount: " + allCount_i18n_status + " hitCount: " +
                (allCount_i18n_status - missCount_i18n_status));
        LOG.info("------UIDCACHE-------- cache hit sof_newhpnt: allCount: " + allCount_sof_newhpnt + " hitCount: " +
                (allCount_sof_newhpnt - missCount_sof_newhpnt));

    }

    public void resetStats() throws SQLException {
        allCount = 0;
        missCount = 0;

        allCount_v9_v9 = 0;
        allCount_sof_dsk = 0;
        allCount_govome = 0;
        allCount_globososo = 0;
        allCount_sof_newgdp = 0;
        allCount_i18n_status = 0;
        allCount_sof_newhpnt = 0;

        missCount_v9_v9 = 0;
        missCount_govome = 0;
        missCount_globososo = 0;
        missCount_sof_dsk = 0;
        missCount_sof_newgdp = 0;
        missCount_i18n_status = 0;
        missCount_sof_newhpnt = 0;

        getUidTime = new ConcurrentHashMap<String, Long>();
        for (Map.Entry<String, Map<String, Connection>> entry : pidNodeConnections.entrySet()) {
            closeOneProjectConnection(entry.getKey());
        }
    }


    private boolean checkInHot(String project, long innerUid) throws SQLException {
        Connection conn;
        Statement stat = null;
        ResultSet resultSet = null;
        try {
            conn = getUidConn(project, innerUid);
            stat = conn.createStatement();
            resultSet = stat.executeQuery("select uid from `register_time` where uid=" + UidMappingUtil.getInstance().decorateWithMD5(innerUid) + ";");
            return resultSet.next();
        } finally {
            if (resultSet != null)
                resultSet.close();
            if (stat != null)
                stat.close();
        }
    }



    private Connection getUidConn(String project, long innerUid) throws SQLException {
        Map<String, Connection> nodeConnections = pidNodeConnections.get(project);
        if (nodeConnections == null) {
            nodeConnections = new HashMap<String, Connection>();
            pidNodeConnections.put(project, nodeConnections);
        }
        String nodeAddress = UidMappingUtil.getInstance().hash(innerUid);
        Connection connection = nodeConnections.get(nodeAddress);
        if (connection == null) {
            connection = MySql_16seqid.getInstance().getConnByNode(project, nodeAddress);
            nodeConnections.put(nodeAddress, connection);
        }
        return connection;
    }

    public void closeOneProjectConnection(String project) throws SQLException {
        Map<String, Connection> conections = pidNodeConnections.get(project);
        if (conections != null) {
            pidNodeConnections.remove(project);
            for (Map.Entry<String, Connection> entry : conections.entrySet()) {
                entry.getValue().close();
            }
        }
    }

}

