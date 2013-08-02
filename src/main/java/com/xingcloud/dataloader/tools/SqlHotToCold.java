package com.xingcloud.dataloader.tools;

import com.xingcloud.dataloader.buildtable.mapreduce.BuildUtil;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.util.Log4jProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Author: qiujiawei
 * Date:   12-7-18
 */
public class SqlHotToCold {

    public static final Log LOG = LogFactory.getLog(SqlHotToCold.class);

    static public void main(String[] args) {
        Log4jProperties.init();
        SqlHotToCold buildDeuJob = new SqlHotToCold();
        String beginProject = null;
        if (args.length > 1 && args[0] != null) {
            long start = System.currentTimeMillis();
            Set<String> projects;
            if (args[0].equals("all") || args[0].startsWith("begin:")) {
                projects = BuildUtil.getProjectList();
            } else {
                projects = new HashSet<String>();
                projects.add(args[0]);
            }

            if (args[0].startsWith("begin")) {
                beginProject = args[0].substring(6);
            }

            List<String> sortList = new ArrayList<String>();
            List<String> runList = new ArrayList<String>();
            for (String project : projects) sortList.add(project);
            Collections.sort(sortList);

            for (String project : sortList) {
                if (beginProject == null || project.compareTo(beginProject) >= 0) {
                    runList.add(project);
                }
            }
            LOG.info("ready to run:" + runList);
            for (String project : runList) {
                buildDeuJob.run(project, args[1]);
            }
            long end = System.currentTimeMillis();
            LOG.info("all job finish using" + (end - start) / 1000);
        }
    }

    public void run(String project, String lastLoginTime) {

        try {
            while (MySql_16seqid.getInstance().hot2cold(project, lastLoginTime) > 0) {
                //do nothing
            }
            LOG.info("MySql_16seqid hot2cold" + project + "finish");
        } catch (Exception e) {
            LOG.error(e);
        }
    }
}
