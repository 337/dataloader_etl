package com.xingcloud.dataloader;

import com.xingcloud.dataloader.hbase.table.DeuTable;
import com.xingcloud.dataloader.lib.HdfsPath;
import com.xingcloud.hbase.HBaseOperation;
import com.xingcloud.id.c.IdClient;
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
import redis.clients.jedis.ShardedJedis;

import java.io.IOException;
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


    private static void printUsage() {
        System.out.println("Usage:\n" +
            "\tbuild project\n" +
            "\tbuild_sql project\n" +
            "\tbuild_id project\n" +
            "\tbuild_deu project\n" +
            "\tcheck_cache project\n" +
            "\tclear_cache project\n"
        );
    }


    static public void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            return;
        }

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
                    sJedis.set(getCacheKeyForProject(project), "a");
                    LOG.info("set project:" + project);
                }
            } catch (Exception e) {
                LOG.error("catch Exception", e);
                RedisShardedPoolResourceManager.getInstance().returnBrokenResource(sJedis);
            } finally {
                RedisShardedPoolResourceManager.getInstance().returnResource(sJedis);
            }
        } else if (mode.equals("build")) {
            LOG.info("build " + args[1]);
            ensureProject(args[1]);
        } else if (mode.equals("build_sql")) {
            LOG.info("build_sql " + args[1]);
            ensureProjectInMySQL(args[1]);
        } else if (mode.equals("build_id")) {
            LOG.info("build_id " + args[1]);
            ensureProjectInIdService(args[1]);
        } else if (mode.equals("build_deu")) {
            LOG.info("build_deu " + args[1]);
            ensureProjectInHBase(args[1]);
        } else if (mode.equals("check_cache")) {
            LOG.info("check_cache " + args[1]);
            LOG.info(checkProjectInRedis(args[1]));
        } else if (mode.equals("clear_cache")) {
            LOG.info("clear_cache " + args[1]);
            clearProjectInRedis(args[1]);
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
        }

    }


    private static String getCacheKeyForProject(String project) {
        return "ui.check." + project;
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
            if (getShardedJedis().exists(getCacheKeyForProject(project))) {
                projectSet.add(project);
                return true;
            }
            return false;
        }
    }


    private static void clearProjectInRedis(String project) {
        LOG.info("clear cache for project " + project);
        getShardedJedis().del(getCacheKeyForProject(project));
        projectSet.remove(project);
    }


    static void setProjectInRedis(String project) {
        LOG.info("------Begin to set project to redis... " + project);
        String retunStr = getShardedJedis().set(getCacheKeyForProject(project), "a");

        LOG.info("------Set finished. " + retunStr + " " + project);
        projectSet.add(project);
    }


    private static void ensureProjectInHBase(String project) {
        int tryTimes = 1;
        boolean successful = false;
        LOG.info("begin to ensure project " + project + " in hbase");
        long start = System.currentTimeMillis();
        DeuTable deuTable = new DeuTable(project, "20000101");
        while (!successful) {
            try {
                HBaseOperation.getInstance().ensureTable(
                    deuTable.getTableName(project),
                    deuTable.getHTableDescriptor(project)
                );
                successful = true;
            } catch (IOException ioException) {
                LOG.error("ensure project " + project + " in hbase throw exception " +
                    ioException.getMessage(), ioException);
            }

            if (!successful) {
                try {
                    LOG.info("retry in " + Constants.SLEEP_MILLIS_IF_EXCEPTION * tryTimes / 1000 + " seconds");
                    Thread.sleep(Constants.SLEEP_MILLIS_IF_EXCEPTION * tryTimes);
                    tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }

        LOG.info("ensure project " + project + " in hbase finished. " +
            "cost time " + (System.currentTimeMillis() - start) + "ms");
    }


    private static void ensureProjectInIdService(String project) {
        int tryTimes = 1;
        boolean successful = false;
        LOG.info("begin to ensure project " + project + " in id service");
        long start = System.currentTimeMillis();
        while (!successful) {
            try {
                boolean result = IdClient.getInstance().createProject(project);
                if (result) {
                    successful = true;
                } else {
                    LOG.error("ensure project " + project + " in id service return false");
                }
            } catch (Exception ex) {
                LOG.error("create project " + project + " in id service throw exception " +
                    ex.getMessage(), ex);
            }

            if (!successful) {
                try {
                    LOG.info("retry in " + Constants.SLEEP_MILLIS_IF_EXCEPTION * tryTimes / 1000 + " seconds");
                    Thread.sleep(Constants.SLEEP_MILLIS_IF_EXCEPTION * tryTimes);
                    tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }

        LOG.info("ensure project " + project + " in id service finished. " +
            "cost time " + (System.currentTimeMillis() - start) + "ms");
    }


    private static void ensureProjectInMySQL(String project) {
        int tryTimes = 1;
        boolean successful = false;
        LOG.info("begin to ensure project " + project + " in mysql");
        long start = System.currentTimeMillis();
        while (!successful) {
            try {
                MySql_16seqid.getInstance().createDBIfNotExist(project);
                successful = true;
            } catch (SQLException sqlex) {
                LOG.error("ensure project " + project + " in id service throw exception " +
                    sqlex.getMessage(), sqlex);
            }

            if (!successful) {
                try {
                    LOG.info("retry in " + Constants.SLEEP_MILLIS_IF_EXCEPTION * tryTimes / 1000 + " seconds");
                    Thread.sleep(Constants.SLEEP_MILLIS_IF_EXCEPTION * tryTimes);
                    tryTimes = (tryTimes << 1) & Integer.MAX_VALUE;
                } catch (InterruptedException ie) {
                    LOG.error(ie.getMessage(), ie);
                }
            }
        }

        LOG.info("ensure project " + project + " in mysql finished. " +
            "cost time " + (System.currentTimeMillis() - start) + "ms");
    }


    /**
     * 调用这个方法来保证存储(MySQL和HBase)中有和项目对应的信息。
     *
     * @param project 项目ID
     */
    static public void ensureProject(String project) {
        if (!checkProjectInRedis(project)) {
            LOG.info("begin to ensure project: " + project);
            long start = System.currentTimeMillis();

            ensureProjectInIdService(project);
            ensureProjectInHBase(project);
            ensureProjectInMySQL(project);

            long end = System.currentTimeMillis();
            LOG.info("ensure project " + project + " cost time " + (end - start) + "ms");

            setProjectInRedis(project);
        }
    }

    static public void ensureProjectTable(String project) {
        try {
            LOG.info("build project:" + project);
            DeuTable deuTable = new DeuTable(project, "20000101");
            setProjectInRedis(project);
            HBaseOperation.getInstance().ensureTable(deuTable.getTableName(project), deuTable.getHTableDescriptor(project));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void close() {
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
