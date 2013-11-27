package com.xingcloud.dataloader.hbase.table.user;

import com.xingcloud.dataloader.lib.*;
import com.xingcloud.util.Common;
import com.xingcloud.util.Constants;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 封装所有对用户属性进行更新的操作
 * 添加事件public void addEvent(Event event)
 * 1.
 * 2.	将要更新的属性放入user
 * <p/>
 * 持久化用户属性的更新public void flush()
 * 1．	调用UserFlushDb的start
 * 2．	轮询要更新的用户，每个都调用UserFlushDb的processUser
 * 3．	调用UserFlushDb的flush
 * 4．	调用UserFlushDb的end
 * <p/>
 * <p/>
 * <p/>
 * <p/>
 * <p/>
 * <p/>
 * Author: qiujiawei
 * Date:   12-4-28
 */

public class UserProxy {
    public static final Log LOG = LogFactory.getLog(UserProxy.class);

    protected String project;
    private String date;
    private int index;

    protected Map<Long, User> userMap = new HashMap<Long, User>();
    protected Map<Long, User> delayUserMap = new HashMap<Long, User>();

    private ProjectPropertyCache projectPropertyCache = null;


    //设置redis里面vist uid需要的一些变量
    private int visitRedisIndex = Constants.REDIS_DB_NUM;
    private int dateSubPos = 8;
    private int expireSeconds = 24 * 3600;

    private String visitJedisKeyPart = "_visitTodayUids_";


    public UserProxy(String project, String date, int index) {
        this.project = project;
        this.date = date;
        this.index = index;
        this.projectPropertyCache = ProjectPropertyCache.getProjectPropertyCacheFromProject(project);
    }

    public String getProject() {
        return project;
    }

    private long getUserSeqUid(Event event) throws Exception {
        Long seqUid = event.getSeqUid();
        if (seqUid == null) {
            seqUid = (long)SeqUidCacheMapV2.getInstance().getUidCache(project, event.getUid());
            event.setSeqUid(seqUid);
        }
        return seqUid;
    }

    public void addEvent(Event event) throws Exception {
        User tempUser = null;
        if (event.getEvent().startsWith("visit.")) {
            long seqUid = getUserSeqUid(event);
            tempUser = getUser(seqUid,event.getTimestamp());
            //如果为visit事件，更新注册时间和最后登陆时间
            User.updatePropertyIntoUser(project, tempUser, projectPropertyCache.getUserPro(User.registerField),
                    User.registerField, Common.getLongPresentByTimestamp(event.getTimestamp()));
            User.updatePropertyIntoUser(project, tempUser, projectPropertyCache.getUserPro(User.lastLoginTimeField),
                    User.lastLoginTimeField, Common.getLongPresentByTimestamp(event.getTimestamp()));
        } else if (event.getEvent().startsWith("pay.gross.") || event.getEvent().equals("pay.")) {
            long seqUid = getUserSeqUid(event);
            tempUser = getUser(seqUid,event.getTimestamp());
            //如果为pay事件，更新首次支付时间和最后支付时间，支付
            User.updatePropertyIntoUser(project, tempUser, projectPropertyCache.getUserPro(User.firstPayTimeField),
                    User.firstPayTimeField, Common.getLongPresentByTimestamp(event.getTimestamp()));
            User.updatePropertyIntoUser(project, tempUser, projectPropertyCache.getUserPro(User.lastPayTimeField),
                    User.lastPayTimeField, Common.getLongPresentByTimestamp(event.getTimestamp()));
            User.updatePropertyIntoUser(project, tempUser, projectPropertyCache.getUserPro(User.payAmountField),
                    User.payAmountField, event.getValue());
        } else if (event.getEvent().equals("update.")) {
            long seqUid = getUserSeqUid(event);
            tempUser = getUser(seqUid,event.getTimestamp());
            try {
                JSONObject jsonObject = JSONObject.fromObject(event.getJson());
                for (Object key : jsonObject.keySet()) {
                    //依次更新所有属性
                    String userPropertyName = (String) key;
                    if (projectPropertyCache.getUserPro(userPropertyName) == null)
                        continue;
                    User.updatePropertyIntoUser(project, tempUser, projectPropertyCache.getUserPro(userPropertyName),
                            userPropertyName, jsonObject.get(userPropertyName));
                }
            } catch (JSONException je) {
                LOG.warn("json format error:" + je.getMessage() + "\t" + event + ", at project:" + project);
            } catch (Exception e) {
                LOG.debug("update catch Exception ", e);
            }
        }

        //延迟user update的log
        if (tempUser != null && event.getDate().compareTo(this.date) < 0) {
            delayUserMap.put(tempUser.getSeqUid(), tempUser);
        }

    }


    /**
     * 根据事件，获取userMap中对应的用户user
     *
     * @param seqUid 根据userMap获取对应的用户
     * @return 返回要更新的属性的user
     */
    private User getUser(long seqUid,long timestamp) {
        User value = userMap.get(seqUid);
        if (value == null) {
            value = new User(seqUid,timestamp);
            userMap.put(seqUid, value);
        }
        return value;
    }

    public Map<Long, User> getPutUsers() {
        return userMap;
    }

    public Map<Long, User> getDelayUsers() {
        return delayUserMap;
    }


}
