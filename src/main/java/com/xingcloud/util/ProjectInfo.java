package com.xingcloud.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.xingcloud.dataloader.BuildTableAdmin;
import com.xingcloud.dataloader.driver.MongodbDriver;
import com.xingcloud.util.http.HttpRequester;
import com.xingcloud.util.http.HttpRespons;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 保存所有合法的项目的信息，包括ID，语言，平台，等等。
 * 并提供快速查询的方法getProjectInfoFromAppidOrProject() 来判断一个appid或者projectid是否是有登记的合法的项目。
 * <p/>
 * 合法的项目的来源有：
 * 1. 从p.xingcloud.com同步过来的合法项目id;
 * 2. 一些遗留项目的信息。保存在 legacyApps 里面。
 * <p/>
 * 同时屏蔽掉一些不合法的过期项目。保存在 ignoreProjects 里面。
 * <p/>
 * Author: qiujiawei
 * Date:   12-4-20
 */
public class ProjectInfo {
  public static final Log LOG = LogFactory.getLog(ProjectInfo.class);
  private String project;
  private String language;
  private String platform;
  private String identifier;

  private ProjectInfo(String project, String platform, String language, String identifier) {
    this.project = project;

    this.language = mapChange(languageMap, language);

    this.platform = mapChange(platformMap, platform);

    this.identifier = identifier;
  }

  public String getProject() {
    return project;
  }

  public String getLanguage() {
    return language;
  }

  public String getPlatform() {
    return platform;
  }

  public String getIdentifier() {
    return identifier;
  }

  public String toString() {
    return project + "," + platform + "," + language + "," + identifier;
  }

  static Set<String> ignoreProjects = new HashSet<String>() {{
    add("elex-demo-client-zyp");
    add("softwarepublish");
    add("test123");
    add("biz");
    add("test");
    add("updatesdk2");
    add("updatesdk");
    add("test2011");
    add("test827");
    add("updatesdk3");
    add("hr");
    add("a6d676d217b94ac2");
    add("gettingstarted");
    add("test829");
    add("cttest");
    add("buildonmembase");
    add("ismolegirltest");
    add("mtest");
    add("redeployment-demo");
    add("bddemo1");
    add("np");
    add("gdddd");
    add("asd");
    add("client-support");
    add("woerma");
    add("to");
    add("hahahaha");
    add("xingcloud-demo");
    add("vvvv9");
    add("b-elex");
    add("a123456789b");
    add("monitor");
    add("qiao-test-01");
    add("testgame");
    add("sdk1");
    add("zhendema");
    add("shiyan1");
    add("win");
    add("learnxingcloud");
    add("m-sns");
    add("clouds");
    add("chucktest");
    add("jw-beta1");
    add("xingzheng");
    add("shiyan4");
    add("sddtools");
    add("testpublish");
    add("finance");
    add("sdk");
    add("task");
    add("wy");
    add("bd2");
    add("bcde");
    add("gamedesign");
    add("337bd");
    add("payelex");
    add("translation");
    add("cszdcj");
    add("newsdk");
    add("sgp");
    add("credits337");
    add("idetest");
    add("2q3e3e");
    add("testsdk");
    add("back");
    add("ceshi");
    add("dhb");
    add("start");
    add("publish");
    add("xingcloud-business");
    add("xct");
    add("learn-xingcloud-2");
    add("chuan123");
    add("xin");
    add("heshixi-test01");
    add("ngame-payment");
    add("t");
    add("uol");
    add("project");
    add("intranetproject");
    add("shine-testproject");
    add("fbmarket");
    add("ide");
    add("product-center");
    add("fortest");
    add("webgames");
    add("game321");
    add("x23324");
    add("110816-1");
    add("yyyyy");
    add("rome");
    add("soft");
    add("connect");
    add("globalpublish");
    add("xulin-test");
    add("dev");
    add("xiaoyouxi");
    add("new-sample-code4");
    add("a5646465464564add");
    add("fx");
    add("defender");
    add("nztx");

  }};

  static List<String> ignoreProjectsInMongo = getIgnoreProjectsFromMongodb();

  static String mapChange(Map<String, String> map, String key) {
    String re = map.get(key);
    if (re != null) return re;
    else return key;
  }

  static Map<String, String> languageMap = new HashMap<String, String>() {{
    put("zh", "cn");
    put("jp", "ja");
    put("us", "en");
    put("vi", "vn");
  }};
  static Map<String, String> platformMap = new HashMap<String, String>() {{
    put("337", "elex337");
    put("vz", "meinvz");
  }};

  static Map<String, ProjectInfo> appid_pid_relationship = new HashMap<String, ProjectInfo>() {{
    put("kldz_meinvz", new ProjectInfo("kldz", "meinvz", "de", "kldz_meinvz"));
    put("farmorkut_en", new ProjectInfo("happyfarmer", "orkut", "en", "farmorkut_en"));
    put("farmfacebook_ml", new ProjectInfo("happyfarmer", "facebook", "ms", "farmfacebook_ml"));//马来西亚语
    put("farmsonico_1", new ProjectInfo("happyfarmer", "sonico", "en", "farmsonico_1"));
    put("farmfacebook_arab", new ProjectInfo("happyfarmer", "facebook", "ar", "farmfacebook_arab"));
    put("farmmeinvz", new ProjectInfo("happyfarmer", "meinvz", "de", "farmmeinvz"));
    put("farmfacebook_de", new ProjectInfo("happyfarmer", "facebook", "de", "farmfacebook_de"));
    put("farmhyves_1", new ProjectInfo("happyfarmer", "hyves", "nl", "farmhyves_1"));
    put("farmfacebook_pt", new ProjectInfo("happyfarmer", "facebook", "pt", "farmfacebook_pt"));
    put("farmnk", new ProjectInfo("happyfarmer", "nk", "pl", "farmnk"));
    put("farm_facebook_tw", new ProjectInfo("happyfarmer", "facebook", "tw", "farm_facebook_tw"));
    put("farmorkut_en", new ProjectInfo("happyfarmer", "orkut", "en", "farmorkut_en"));
    put("farmfacebook_tr", new ProjectInfo("happyfarmer", "facebook", "tr", "farmfacebook_tr"));
    put("farmlokalisten", new ProjectInfo("happyfarmer", "lokalisten", "de", "farmlokalisten"));
    put("farmfacebook_th_1", new ProjectInfo("happyfarmer", "facebook", "th", "farmfacebook_th_1"));
    put("farmfacebook_th_test", new ProjectInfo("happyfarmer", "facebook", "th", "farmfacebook_th_test"));
    put("farmfacebook_nl", new ProjectInfo("happyfarmer", "facebook", "nl", "farmfacebook_nl"));
    put("casino_meinvz", new ProjectInfo("casino", "meinvz", "de", "casino_meinvz"));
    put("casino_facebook", new ProjectInfo("casino", "facebook", "en", "casino_facebook"));
    put("fish_nk", new ProjectInfo("hdzdy", "nk", "pl", "fish_nk"));
    put("qilongjiorkut", new ProjectInfo("qlj", "orkut", "pt", "qilongjiorkut"));
    put("football_orkut", new ProjectInfo("rxzq", "orkut", "en", "football_orkut"));
    put("football_meinvz", new ProjectInfo("rxzq", "meinvz", "de", "football_meinvz"));
    put("football_facebook_de", new ProjectInfo("rxzq", "facebook", "de", "football_facebook_de"));
    put("football_lokalisten", new ProjectInfo("rxzq", "lokalisten", "de", "football_lokalisten"));
    put("football_orkut_pt", new ProjectInfo("rxzq", "orkut", "pt", "football_orkut_pt"));
    put("pandora_test", new ProjectInfo("pandora", "meinvz", "de", "pandora_test"));
    put("cafelife_orkut_pt", new ProjectInfo("cafelife", "orkut", "pt", "cafelife_orkut_pt"));
    put("cafelife", new ProjectInfo("cafelife", "orkut", "pt", "cafelife"));
    put("cafelife_meinvz_de", new ProjectInfo("cafelife", "meinvz", "de", "cafelife_meinvz_de"));
    put("zooworld", new ProjectInfo("zooworld", "meinvz", "de", "zooworld"));
    put("plantmeinvz", new ProjectInfo("cuteplant", "meinvz", "de", "plantmeinvz"));
    put("pigorkut", new ProjectInfo("zhuguan", "orkut", "pt", "pigorkut"));
    put("pigmeinvz", new ProjectInfo("zhuguan", "meinvz", "de", "pigmeinvz"));
    put("pig_lokalisten", new ProjectInfo("zhuguan", "lokalisten", "de", "pig_lokalisten"));
    put("pignl", new ProjectInfo("zhuguan", "hyves", "nl", "pignl"));
    put("pignk", new ProjectInfo("zhuguan", "nk", "pl", "pignk"));
    put("mdxd_meinvz", new ProjectInfo("mhdxd", "meinvz", "de", "mdxd_meinvz"));
    put("swjy_orkut", new ProjectInfo("swjy", "orkut", "pt", "swjy_orkut"));
    put("zzzw_orkut_pt", new ProjectInfo("zzzw", "orkut", "pt", "zzzw_orkut_pt"));
    put("hospital_meinvz", new ProjectInfo("kxyy", "meinvz", "de", "hospital_meinvz"));
    put("hospital_orkut", new ProjectInfo("kxyy", "orkut", "pt", "hospital_orkut"));
    put("hospital_lokalisten", new ProjectInfo("kxyy", "lokalisten", "de", "hospital_lokalisten"));
    put("magicflower_vz", new ProjectInfo("magic-gardener", "meinvz", "de", "magicflower_vz"));
    put("friendtd", new ProjectInfo("friendtd", "facebook", "tw", "friendtd"));
    put("myislandmeinvz", new ProjectInfo("island", "meinvz", "de", "myislandmeinvz"));
    put("myisland_lokalisten", new ProjectInfo("island", "lokalisten", "de", "myisland_lokalisten"));
    put("hitower", new ProjectInfo("soletower", "meinvz", "de", "hitower"));
    put("ranchfacebook", new ProjectInfo("happyranch", "facebook", "tw", "ranchfacebook"));
    put("farm3_meinvz", new ProjectInfo("happyranch", "meinvz", "de", "farm3_meinvz"));
    put("ranchfacebook_de", new ProjectInfo("happyranch", "facebook", "de", "ranchfacebook_de"));
    put("farm3_facebook_de", new ProjectInfo("happyranch", "facebook", "de", "farm3_facebook_de"));
    put("farm3_facebook_tw_test", new ProjectInfo("happyranch", "facebook", "tw", "farm3_facebook_tw_test"));
    put("farm3_meinvz_test", new ProjectInfo("happyranch", "meinvz", "de", "farm3_meinvz_test"));
    put("farm3_facebook_en_test", new ProjectInfo("happyranch", "facebook", "en", "farm3_facebook_en_test"));
    put("farm3_orkut_pt_test", new ProjectInfo("happyranch", "orkut", "pt", "farm3_orkut_pt_test"));
    put("farm3_naszaklasa_pl_test", new ProjectInfo("happyranch", "facebook", "pl", "farm3_naszaklasa_pl_test"));
    put("farm3_lokalisten", new ProjectInfo("happyranch", "lokalisten", "de", "farm3_lokalisten"));
    put("farm3_orkut_pt", new ProjectInfo("happyranch", "orkut", "pt", "farm3_orkut_pt"));
    put("dzpk_orkut", new ProjectInfo("dzpk", "orkut", "pt", "dzpk_orkut"));
    put("dzpk_english", new ProjectInfo("dzpk", "facebook", "en", "dzpk_english"));
    put("dzpk_fanti", new ProjectInfo("dzpk", "facebook", "tw", "dzpk_fanti"));
    put("dzpk_meinvz", new ProjectInfo("dzpk", "meinvz", "de", "dzpk_meinvz"));
    put("mhd", new ProjectInfo("mhc", "orkut", "de", "mhd"));
    put("mhc_meinvz", new ProjectInfo("mhc", "meinvz", "de", "mhc_meinvz"));
    put("mhc_lokalisten", new ProjectInfo("mhc", "lokalisten", "de", "mhc_lokalisten"));
    put("mhc_orkut_pt", new ProjectInfo("mhc", "orkut", "pt", "mhc_orkut_pt"));
    put("foreststory-local@orkut_zh_CN", new ProjectInfo("foreststory_local", "orkut", "cn", "foreststory-local@orkut_zh_CN"));
    put("happyfarmer@facebook_zh_TW", new ProjectInfo("happyfarmer", "facebook", "tw", "happyfarmer@facebook_zh_TW"));
    put("petclub@facebook_zh_TW", new ProjectInfo("petclub", "facebook", "tw", "petclub@facebook_zh_TW"));


    put("ddt_orkut", new ProjectInfo("ddt", "pt", "orkut", "ddt_orkut"));
  }};

  static String[] permitAppidList = new String[]{
          "v9@elex337_pt_1",
          "ddt_orkut"
  };

  /**
   *
   */
  static Set<String> projectSet = null;
  static Set<String> appidSet = null;

  static void initProjectAndAppid() {

    try {
      long t1 = System.currentTimeMillis();
      projectSet = new HashSet<String>();
      appidSet = new HashSet<String>();

      //add permitApppid but not build in the redmine rest api
      Collections.addAll(appidSet, permitAppidList);


      long t2 = System.currentTimeMillis();
      LOG.info("get http using:" + (t2 - t1) + "ms");
      JSONObject json = JSONObject.fromObject(getProjectInfoFromMongodb());


      for (Object keyObj : json.keySet()) {
        if (keyObj instanceof String) {
          String project = (String) keyObj;
          if (ignoreProjects.contains(project)) continue;
          if (ignoreProjectsInMongo.contains(project)) continue;
          projectSet.add(project);
          JSONArray tempAppidList = json.getJSONObject(project).getJSONArray("app_ids");
          for (Object appid : tempAppidList) {
            if (appid instanceof String) {
              appidSet.add((String) ((String) appid).toLowerCase());
            }
          }
        }
      }
      long t3 = System.currentTimeMillis();

      LOG.info("init json using: " + (t3 - t1) + "ms");
    } catch (Exception e) {
      LOG.error("get project list error ", e);
    }
  }

  static public void clear() {
    appidSet = null;
    projectSet = null;
  }

  static public ProjectInfo getProjectInfoFromAppidOrProject(String appid) {
    try {

      if (appidSet == null || projectSet == null) {
        initProjectAndAppid();
      }

      if (appid_pid_relationship.containsKey(appid))
        return appid_pid_relationship.get(appid);

      //special appid for v9
      if (appid.startsWith("v9@337") || appid.startsWith("v9@elex337")) {
        if (appid.matches("^[a-zA-Z0-9]([a-zA-Z0-9_.\\-]*)@([a-zA-Z0-9_.\\-]+)_([a-zA-Z0-9_.\\-]+$)")) {
          int at = appid.indexOf("@");
          int firstUnderscore = appid.indexOf("_", at);
          int secondUnderscore = appid.indexOf("_", firstUnderscore + 1);
          String project = appid.substring(0, at);
          String platform = appid.substring(at + 1, firstUnderscore);
          String language = appid.substring(firstUnderscore + 1, secondUnderscore);
          String identifier = appid.substring(secondUnderscore + 1);
          ProjectInfo projectInfo = new ProjectInfo(project, platform, language, identifier);
          appid_pid_relationship.put(appid, projectInfo);
          return projectInfo;
        }
      }

      if (appid.matches("^[a-zA-Z0-9]([a-zA-Z0-9_.\\-]*)@([a-zA-Z0-9_.\\-]+)_([a-zA-Z0-9_.\\-]+$)")) {
        int at = appid.indexOf("@");
        int firstUnderscore = appid.indexOf("_", at);
        int secondUnderscore = appid.indexOf("_", firstUnderscore + 1);
        String project = appid.substring(0, at);
        if (!projectSet.contains(project))
          return null;
        String platform = appid.substring(at + 1, firstUnderscore);
        String language = null;
        if (secondUnderscore < 0) language = appid.substring(firstUnderscore + 1);
        else language = appid.substring(firstUnderscore + 1, secondUnderscore);
        String identifier = null;
        if (secondUnderscore > 0) identifier = appid.substring(secondUnderscore + 1);
        ProjectInfo projectInfo = new ProjectInfo(project, platform, language, identifier);
        appid_pid_relationship.put(appid, projectInfo);
        return projectInfo;
      }

      if (projectSet.contains(appid)) {
        ProjectInfo projectInfo = new ProjectInfo(appid, null, null, null);
        appid_pid_relationship.put(appid, projectInfo);
        return projectInfo;
      }
    } catch (Exception e) {
      LOG.error("appid is:" + appid, e);
    }
    return null;
  }

  static String getProjectInfoFromRest() {
    try {
      HttpRequester request = new HttpRequester();
      HttpRespons hr = request.sendGet("http://p.xingcloud.com/rest/projects?token=bd602691074e1aabdf818f8b539f1ef7");
      return hr.getContent();
    } catch (Exception e) {
      LOG.error(e);
    }
    return null;
  }

  static String getProjectInfoFromMongodb() {
    try {

      DBCollection coll = MongodbDriver.getInstanceDB().getCollection("project");

      DBObject re = coll.findOne(new BasicDBObject("type", "project"));
      return (String) re.get("info");
    } catch (Exception e) {
      LOG.error(e);
    }
    return null;
  }

  static ArrayList<String> getIgnoreProjectsFromMongodb() {
    try {

      DBCollection coll = MongodbDriver.getInstanceDB().getCollection("project");

      DBObject re = coll.findOne(new BasicDBObject("type", "ignoreProjects"));
      return (ArrayList<String>) re.get("projects");
    } catch (Exception e) {
      LOG.error(e);
    }
    return null;
  }

  static void setIgnoreProjectsIntoMongodb(ArrayList<String> ignoreProjectsList) {
    try {
      DBCollection coll = MongodbDriver.getInstanceDB().getCollection("project");
      DBObject listObject = new BasicDBObject().append("projects", ignoreProjectsList);
      DBObject setObject = new BasicDBObject().append("$set", listObject);
      coll.update(new BasicDBObject().append("type", "ignoreProjects"), setObject);
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  static void storeNewProjectInfoIntoMongodb() {
    try {

      DBCollection coll = MongodbDriver.getInstanceDB().getCollection("project");
      int version = 0;
      DBObject re = coll.findOne(new BasicDBObject("type", "version"));
      if (re != null) {
        version = (Integer) re.get("version");
      }
      HttpRequester request = new HttpRequester();
      HttpRespons hr = request.sendGet("http://basisREQUEST8791:3431940529132@p.xingcloud.com:3001/apps/" + version);
      JSONObject newJson = JSONObject.fromObject(hr.getContent());
      DBObject oldJson = coll.findOne(new BasicDBObject("type", "project"));
      int new_version = newJson.getInt("version");

      for (Object keyObj : newJson.keySet()) {
        if (keyObj instanceof String) {
          String project = (String) keyObj;
          if (project.equals("version")) continue;
          if (ignoreProjects.contains(project)) continue;
//                    oldJson.
//                    JSONArray tempAppidList = newJson.getJSONObject(project).getJSONArray("app_ids");
//                    for(Object appid:tempAppidList){
//                        if(appid instanceof String){
//                            appidSet.add((String)appid);
//                        }
//                    }
        }
      }


      BasicDBObject temp = new BasicDBObject();
      temp.put("info", hr.getContent());
      BasicDBObject updateObject = new BasicDBObject();
      updateObject.put("$set", temp);

      coll.update(new BasicDBObject("type", "project"), updateObject, true, true);
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  static class UpdateTask implements Runnable {

    public void run() {
      try {
        HttpRequester request = new HttpRequester();
        HttpRespons hr = request.sendGet("http://p.xingcloud.com/rest/projects?token=bd602691074e1aabdf818f8b539f1ef7");

        // JSONObject json=JSONObject.fromObject(hr.getContent());

        DBCollection coll = MongodbDriver.getInstanceDB().getCollection("project");

        BasicDBObject temp = new BasicDBObject();
        temp.put("info", hr.getContent());

        BasicDBObject updateObject = new BasicDBObject();
        updateObject.put("$set", temp);

        coll.update(new BasicDBObject("type", "project"), updateObject, true, true);
      } catch (Exception e) {
        LOG.error(e);
      }
    }
  }

  static void storeProjectInfoIntoMongodb() {
    try {
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      executorService.execute(new UpdateTask());
      executorService.shutdown();
      if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        executorService.shutdownNow();
        LOG.info("update failed");
      } else {
        LOG.info("update successful");
      }
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  static public ProjectInfo getRanchfacebookProject() {
    return new ProjectInfo("ranchfacebook", null, null, null);
  }

  /**
   * 关于项目信息的工具入口。
   * <p/>
   * update：从p.xingcloud.com同步项目信息到mongodb中去。
   * <p/>
   * 定期执行，目前每5分钟执行一次。
   * <p/>
   * clear: 根据mongodb中的合法项目列表，项目清除 hdfs上的废旧appid所对应的目录。
   * <p/>
   * 线下维护用。如果需要，可以运行一下。
   * <p/>
   * check: 检查某个appid是否生效。
   * <p/>
   * 作调试之用。
   *
   * @param args
   */
  static public void main(String[] args) {
    Log4jProperties.init();
    if (args.length == 0)
      ProjectInfo.initProjectAndAppid();
    else {
      String type = args[0];


      if (type.equals("clear")) {
        try {
          List<String> appidList = new ArrayList<String>();
          String pathRoot = "hdfs://namenode.xingcloud.com:19000/user/hadoop/analytics/";
          FileSystem fs = FileSystem.get(new URI(pathRoot), new Configuration());
          for (FileStatus status : fs.listStatus(new Path(pathRoot))) {
            Path temp = status.getPath();
            if (!fs.isFile(temp)) {
              String appid = temp.getName();
              ProjectInfo projectInfo = ProjectInfo.getProjectInfoFromAppidOrProject(appid);
              if (projectInfo != null) {
                appidList.add(appid);
              } else {
                LOG.info("delete Path:" + temp);
                fs.delete(temp, true);
              }
            }
          }
        } catch (Exception e) {
          LOG.error("catch Exception", e);
        }
      } else if (type.equals("update")) {
        storeProjectInfoIntoMongodb();
//            System.out.println(getProjectInfoFromMongodb());
//            System.out.println(ProjectInfo.getProjectInfoFromAppidOrProject("ranchfacebook"));
      } else if (type.equals("check")) {
        String appid = args[1];
        ProjectInfo projectInfo = ProjectInfo.getProjectInfoFromAppidOrProject(appid);
        System.out.println(ProjectInfo.getProjectInfoFromAppidOrProject(appid));
        BuildTableAdmin.check(projectInfo.getProject());
      } else if (type.equals("ignore")) {
        ArrayList<String> ignoreProjects = ProjectInfo.getIgnoreProjectsFromMongodb();
        String projectList = args[1];
        Object[] projectArray = projectList.split("\\|");
        for (int i = 0; i < projectArray.length; i++) {
          Object o = projectArray[i];
          String project = (String) o;
          if (!ignoreProjects.contains(project)) ignoreProjects.add(project);
        }
        ProjectInfo.setIgnoreProjectsIntoMongodb(ignoreProjects);
      } else if (type.equals("removeFromIgnore")) {
        ArrayList<String> ignoreProjects = ProjectInfo.getIgnoreProjectsFromMongodb();
        String projectList = args[1];
        Object[] projectArray = projectList.split("\\|");
        for (int i = 0; i < projectArray.length; i++) {
          Object o = projectArray[i];
          String project = (String) o;
          if (ignoreProjects.contains(project)) ignoreProjects.remove(project);
        }
        ProjectInfo.setIgnoreProjectsIntoMongodb(ignoreProjects);
      } else if (type.equals("clearIgnore")) {
        ProjectInfo.setIgnoreProjectsIntoMongodb(new ArrayList<String>());
      } else if (type.equals("test")) {
        String appid = args[1];
        ProjectInfo projectInfo = ProjectInfo.getProjectInfoFromAppidOrProject(appid);
        System.out.println(ProjectInfo.getProjectInfoFromAppidOrProject(appid));

      }
    }
  }
}
