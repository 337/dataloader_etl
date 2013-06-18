package com.xingcloud.dataloader.server;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.dataloader.driver.MongodbDriver;
import com.xingcloud.dataloader.lib.TimeIndex;
import com.xingcloud.dataloader.server.message.DataLoaderMessage;
import com.xingcloud.dataloader.server.message.MessageFactory;
import com.xingcloud.dataloader.server.message.TaskMessage;
import com.xingcloud.util.Constants;
import com.xingcloud.util.Log4jProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 消息创作函数，根据传参生成不同类型的任务函数，并添加到相应的任务队列中
 * Author: qiujiawei
 * Date:   12-5-17
 */
public class DataLoaderMessageMaker {
    public static final Log LOG = LogFactory.getLog(DataLoaderMessageMaker.class);
    private static Map<String, WatcherNode> nodeMap = new HashMap<String, WatcherNode>();
    private String hostList;

    public DataLoaderMessageMaker() {
        hostList = StaticConfig.messageHostList;
        LOG.info("init iplist:" + hostList);
        for (String ip : hostList.split(",")) {
            nodeMap.put(ip, new WatcherNode(ip));
        }
    }

    private void process(String message, String ipList) {
        if (ipList.equals("all")) ipList = hostList;
        if (message.equals("nowTask")) nowTask(ipList);
        else if (message.startsWith("delayTask")) delayTask(message, ipList);
        else if (message.startsWith("fillTask") || message.startsWith("fixTask")) {
            String[] t = message.split(",");
            String date = t[1];
            if (date.equals("today")) date = TimeIndex.getDate();

            String indexPair = t[2];
            String[] indexPairArray = indexPair.split("-");
            int si = Integer.valueOf(indexPairArray[0]);
            int ei = 0;
            if (indexPairArray[1].equals("mi")) ei = TimeIndex.getTimeIndex();
            else if (indexPairArray[1].equals("m2i")) ei = TimeIndex.getTimeIndex() - 2;
            else ei = Integer.valueOf(indexPairArray[1]);

            String projectList = t[3];
            String type = t[4];
            for (String ip : ipList.split(",")) {
                if (message.startsWith("fillTask")) fillTask(date, si, ei, projectList, type, ip);
                if (message.startsWith("fixTask")) fixTask(date, si, ei, projectList, type, ip);

            }
        } else addMessageToNodes(MessageFactory.getMessage(message), ipList);
    }

    private void delayTask(String message, String ipList) {
        int delay = Integer.valueOf(message.split(",")[1]);

        String date = TimeIndex.getDate();
        int index = TimeIndex.getTimeIndex();

        index += delay;

        if (index < 0) {
            index += 287;
            date = CalDay(date, -1);
        }


        addMessageToNodes(new TaskMessage(date, index, "all", "Normal", "Low"), ipList);
    }

    private static String CalDay(String date, int dis) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

            sdf.setTimeZone(Constants.TMZ);
            Date temp = sdf.parse(date);

            java.util.Calendar ca = Calendar.getInstance(new SimpleTimeZone(0,
                    Constants.TMZ.getID()));
            ca.setTime(temp);
            ca.add(Calendar.DAY_OF_MONTH, dis);
            return sdf.format(ca.getTime());
        } catch (Exception e) {
            LOG.error(e);
        }
        return null;
    }

    private void printMessage() {
        for (String ip : hostList.split(",")) {
            LOG.info("ip:" + ip + " " + nodeMap.get(ip).getMessageQueue().getAllMessage());
        }
    }

    //assign all the task to all node and check weather the task have run
    public void fillTask(String date, int si, int ei, String projectList, String runType, String ip) {

        int[] all = new int[290];
        DBCursor cursor = MongodbDriver.getFinishStats(date, ip);
        while (cursor.hasNext()) {
            DBObject temp = cursor.next();
            int i = (Integer) temp.get("index");
            all[i]++;
        }
        for (int i = si; i <= ei; i++) {
            if (all[i] == 0) addMessageToNode(new TaskMessage(date, i, projectList, runType, "High"), ip);
        }
    }

    //assign all the task to all node
    public void fixTask(String date, int si, int ei, String projectList, String runType, String ip) {
        for (int i = si; i <= ei; i++) {
            addMessageToNode(new TaskMessage(date, i, projectList, runType, "Low"), ip);
        }
    }

    public void nowTask(String ipList) {
        addMessageToNodes(new TaskMessage(TimeIndex.getDate(), TimeIndex.getTimeIndex(), "all", "Normal", "High"),
                ipList);    }

    /**
     * add one message to one node
     *
     * @param message
     * @param ip
     */
    public void addMessageToNode(DataLoaderMessage message, String ip) {
        boolean first = MessageFactory.getFirst(message);
        LOG.info("add message:" + message + " " + ip);
        if (first) nodeMap.get(ip).getMessageQueue().addMessageFirst(message);
        else nodeMap.get(ip).getMessageQueue().addMessageLast(message);
    }

    /**
     * add one message to all node
     *
     * @param message
     */
    public void addMessageToNodes(DataLoaderMessage message, String ipList) {
        for (String ip : ipList.split(",")) {
            addMessageToNode(message, ip);
        }

    }


    /**
     * 消息格式
     * format:
     * message@ip
     * message example:
     * <p/>
     * printTask@all
     * <p/>
     * <p/>
     * exit@all
     * waitAndExit@all
     * <p/>
     * task,date,index,project,type@all   提交指定日期，时间段，运行项目，任务类型到所有的ip消息队列上
     * <p/>
     * <p/>
     * nowTask@all 提交当前5分钟的任务到所有的ip上
     * <p/>
     * <p/>
     * 根据mongodb的完成记录，填充今天的0到mi段未完成的任务
     * fillTask,date,minInedx-maxIndex,project,type@all
     * fillTask,today,0-mi,all,Normal@ip1，ip2
     * fillTask,20120612,0-mi,all,EventOnly@ip
     * <p/>
     * <p/>
     * <p/>
     * 强制提交任务
     * fixTask,20120611,0-100,all,UserOnly@ip
     * <p/>
     * <p/>
     * <p/>
     * 项目:all or finshman
     * 任务类型:Normal,EventOnly,UserOnly
     * ip:all or 192.168.1.142，192.168.1.143...
     *
     * @param args 0 look like above
     */

    static public void main(String[] args) {
//        String temp=args[0];
//        String temp="printTask@all";
//        String temp="exit@all";
//        args=new String[]{"fixTask,20120619,0-1,tencent-18894,UserOnly@all"};
        DataLoaderMessageMaker dataLoaderMessageMaker = new DataLoaderMessageMaker();
        dataLoaderMessageMaker.printMessage();
        Log4jProperties.init();
        for (String temp : args) {
            String[] t = temp.split("@");
            dataLoaderMessageMaker.process(t[0], t[1]);
        }
    }


}

