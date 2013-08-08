package com.xingcloud.dataloader.lib;


import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 用户属性缓存类，由此确定某项目的某
 * Author: qiujiawei
 * Date:   12-6-5
 */
public class ProjectPropertyCache {
  public static final Log LOG = LogFactory.getLog(ProjectPropertyCache.class);
  private String name = null;
  private List<UserProp> list = null;

  private ProjectPropertyCache(String name, List<UserProp> list) {
    this.name = name;
    this.list = list;
  }

  public UserProp getUserPro(String key) {
    if (list != null) {
      for (UserProp userProp : list) {
        if (userProp.getPropName().equals(key)) return userProp;
      }
    }
    return null;
  }

  static Map<String, ProjectPropertyCache> cache = new ConcurrentHashMap<String, ProjectPropertyCache>();

  static public ProjectPropertyCache getProjectPropertyCacheFromProject(String project) {
    ProjectPropertyCache projectPropertyCache = cache.get(project);
    if (projectPropertyCache == null) {
      List<UserProp> locallist = null;
      try {
        locallist = MySql_16seqid.getInstance().getUserProps(project);
        projectPropertyCache = new ProjectPropertyCache(project, locallist);
        LOG.info("get getProjectPropertyCacheFromProject for "+project );

      } catch (Exception e) {
        LOG.error("MySql_16seqid getProjectPropertyCacheFromProject " + project, e);
        projectPropertyCache = new ProjectPropertyCache(project, null);
      }
      cache.put(project, projectPropertyCache);
    }
    return projectPropertyCache;
  }

  static public void clearCache() {
    cache = new ConcurrentHashMap<String, ProjectPropertyCache>();
  }


  public static void main(String[] args) throws SQLException {
    System.out.println(UidMappingUtil.getInstance().nodes());
    for (UserProp userProp : MySql_16seqid.getInstance().getUserProps("web337")) {
      System.out.println(userProp.getPropName());

    }
  }
}
