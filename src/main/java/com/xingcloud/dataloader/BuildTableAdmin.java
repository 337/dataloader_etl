package com.xingcloud.dataloader;

import com.xingcloud.dataloader.hbase.table.DeuTable;
import com.xingcloud.dataloader.lib.HdfsPath;
import com.xingcloud.hbase.HBaseOperation;
import com.xingcloud.id.c.IDClient;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.redis.RedisShardedPoolResourceManager;
import com.xingcloud.util.Constants;
import com.xingcloud.util.ProjectInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableExistsException;
import redis.clients.jedis.ShardedJedis;

import java.net.URI;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 * 实用工具：
 * init_redis 在redis记录ui.check
 * Author: qiujiawei
 * Date:   12-4-28
 */
public class BuildTableAdmin {


    public static final Log LOG = LogFactory.getLog(BuildTableAdmin.class);
    public static final int UI_CHECK_REDIS_DB = Constants.REDIS_DB_NUM;
    public static ShardedJedis shardedJedis = null;
    private static Set<String> projectSet = new HashSet<String>();

    static public void main(String[] args) {
        String mode = args[0];
        if (mode.equals("init_redis")) {
            ShardedJedis sJedis = null;
            try {
                Set<String> projectSet = new HashSet<String>();
                String hdfsRoot = "hdfs://namenode.xingcloud.com:19000";
                String pathRoot = hdfsRoot + "/user/hadoop/analytics/";
              Configuration configuration = new Configuration();
              configuration.set("fs.default.name","hdfs://namenode.xingcloud.com:19000");
                FileSystem fs = FileSystem.get(new URI(pathRoot), configuration);
                for (FileStatus status : fs.listStatus(new Path(pathRoot))) {
                    Path temp = status.getPath();
                    if (!fs.isFile(temp)) {
                        String appid = temp.getName();
                        ProjectInfo projectInfo = ProjectInfo.getProjectInfoFromAppidOrProject(appid);
                        if (projectInfo != null) {
                            projectSet.add(projectInfo.getProject());
                        } else {
                            LOG.info("delete Path:" + temp);
                            //fs.delete(temp,true);
                        }
                    }
                }
                LOG.info("init project redis:" + projectSet);

                sJedis = RedisShardedPoolResourceManager.getInstance().getCache(UI_CHECK_REDIS_DB);
                for (String project : projectSet) {
                    sJedis.set("ui.check." + project, "a");
                    LOG.info("set project:" + project);
                }
            } catch (Exception e) {
                LOG.error("catch Exception", e);
                RedisShardedPoolResourceManager.getInstance().returnBrokenResource(sJedis);
            } finally {
                RedisShardedPoolResourceManager.getInstance().returnResource(sJedis);
            }
        } else if (mode.equals("build")) {
            ensureProject(args[1]);
            LOG.info("build" + args[1]);
        } else if (mode.equals("build_table")) {
            ensureProjectTable(args[1]);
            LOG.info("build_table" + args[1]);
        } else if (mode.equals("build_sql")) {
            try {
                MySql_16seqid.getInstance().createDBIfNotExist(args[1]);
                LOG.info("MySql_16seqid create mysql table");
            } catch (Exception e) {
                LOG.error("MySql_16seqid" + e);
            }


        } else if (mode.equals("alter_sql")) {
            try {

            } catch (Exception e) {
                LOG.error(e);
            }
        } else if (mode.equals("init_project")) {
            boolean del = false;
            if (args.length > 2 && args[1] != null && args[1].equals("true")) {
                del = true;
            }
            try {
                Set<String> projectSet = new HashSet<String>();
                String pathRoot = HdfsPath.hdfsRoot + "/user/hadoop/analytics/";
                FileSystem fs = FileSystem.get(new URI(pathRoot), new Configuration());
                for (FileStatus status : fs.listStatus(new Path(pathRoot))) {
                    Path temp = status.getPath();
                    if (!fs.isFile(temp)) {
                        String appid = temp.getName();
                        ProjectInfo projectInfo = ProjectInfo.getProjectInfoFromAppidOrProject(appid);

                        if (projectInfo != null) {
                            projectSet.add(projectInfo.getProject());
                        } else {
                            LOG.info("delete Path:" + temp);
                            if (del) fs.delete(temp, true);
                        }
                    }
                }
                LOG.info("project is" + projectSet);


            } catch (Exception e) {
                LOG.error("catch Exception", e);
            }

        } else if (mode.equals("pre_clean")) {
            cleanLocalAppid(false);
        } else if (mode.equals("clean_local")) {
            LOG.info("think and wait for 10second.you should make sure you run pre_clean mode before");
            try {
                Thread.sleep(10000);
                cleanLocalAppid(true);
            } catch (InterruptedException e) {
                e.printStackTrace();  //e:
            }

        }

    }

    private static void cleanLocalAppid(boolean del) {
        //TODO
    }

    static ShardedJedis getShardedJedis() {
        if (shardedJedis == null) {
            shardedJedis = RedisShardedPoolResourceManager.getInstance().getCache(UI_CHECK_REDIS_DB);
        }
        return shardedJedis;
    }

    static boolean checkProjectInRedis(String project) {
        if (projectSet.contains(project)) return true;
        else {
            if (getShardedJedis().exists("ui.check." + project)) {
                projectSet.add(project);
                return true;
            }
            return false;
        }
    }

    static void setProjectInRedis(String project) {
        LOG.info("------Begin to set project to redis... " + project);
        String retunStr = getShardedJedis().set("ui.check." + project, "a");

        LOG.info("------Set finished. " + retunStr + " " + project);
        projectSet.add(project);
    }

    static long checkTable = 0;

    /**
     * 调用这个方法来保证存储(MySQL和HBase)中有和项目对应的信息。
     *
     * @param project 项目ID
     */
    static public void ensureProject(String project) {
        try {
            long start = System.currentTimeMillis();
            if (!checkProjectInRedis(project)) {
                LOG.info("build project:" + project);
                DeuTable deuTable = new DeuTable(project, "20000101");
                try {
                    HBaseOperation.getInstance().ensureTable(deuTable.getTableName(project), deuTable.getHTableDescriptor(project));
//                    HBaseOperation.getInstance().ensureTable(deuTable.getFixTableName(project), deuTable.getFixHTableDescriptor(project));
                } catch (TableExistsException e) {
                    LOG.info("Other dataloader has created this hbase table.");
                }
                IDClient.getInstance().createdb(project);
                int tryCount = 3;
                //三次尝试建库，不成功则发邮件
                for (int i = 0; i < tryCount; i++) {
                    try {
                        MySql_16seqid.getInstance().createDBIfNotExist(project);
                        break;
                    } catch (Exception e) {
                        LOG.error("Mysql ensureProject create  db failed. " + e.getMessage());
                        if (i >= tryCount - 1)
                            throw e;
                    }
                }

                setProjectInRedis(project);

            }
            long end = System.currentTimeMillis();
            checkTable += end - start;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static public void ensureProjectTable(String project) {
        try {
            long start = System.currentTimeMillis();
            LOG.info("build project:" + project);
            DeuTable deuTable = new DeuTable(project, "20000101");
            setProjectInRedis(project);
            HBaseOperation.getInstance().ensureTable(deuTable.getTableName(project), deuTable.getHTableDescriptor(project));
            long end = System.currentTimeMillis();
            checkTable += end - start;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void close() {
        LOG.info("ensure project using:" + checkTable);
        if (shardedJedis != null) {
            RedisShardedPoolResourceManager.getInstance().returnResource(shardedJedis);
        }
    }

    public static void check(String project) {
        try {
            if (checkProjectInRedis(project)) System.out.print("redis exist");
            else System.out.print("redis not exist");
            DeuTable deuTable = new DeuTable(project, "20000101");
            HBaseOperation.getInstance().checkTable(deuTable.getTableName(project));
        } catch (Exception e) {
            LOG.error("check table catch Exception", e);
        }
    }
}
