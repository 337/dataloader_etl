package com.xingcloud.dataloader.lib;

import com.xingcloud.util.Common;
import com.xingcloud.util.manager.DateManager;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 事件数据结构
 * Author: qiujiawei
 * Date:   12-2-29
 */
public class Event {
    private String uid;
    private long timestamp;
    //事件数组
    private String[] eventStr;
    //事件值
    private long value;

    //拓展项，用户update事件
    private String json;

  private int eventLength = 256;

    public final static int eventFieldLength = 6;

    // 序列化后的rowkey
    private byte[] rowKey;

    //seq以后的uid
    private Long seqUid;

    public Event(String uid, String[] eventStr, long value, long timestamp) {
        init(uid, eventStr, value, timestamp, null);
    }

    public Event(String uid, String[] eventStr, long value, long timestamp, String json) {
        init(uid, eventStr, value, timestamp, json);
    }

    public void init(String uid, String[] event_Str, long value, long timestamp, String json) {
        this.uid = StaticFunction.ensureLength(uid, 60);
        this.eventStr = filterInvalidChar(event_Str);
        this.value = value;
        this.timestamp = timestamp;
        this.json = json;
    }

    private String[] filterInvalidChar(String[] event) {
        String[] tmpEvents = new String[event.length];
        for (int i = 0; i < tmpEvents.length; i++) {
            if (event[i] == null)
                break;

            tmpEvents[i] =event[i].replaceAll("[^\\w]","");
//            tmpEvents[i] = event[i].replaceAll(String.valueOf((char) 255), "");
//            tmpEvents[i] = tmpEvents[i].replaceAll("\\*", "");
//            tmpEvents[i] = tmpEvents[i].replaceAll(",", "");

        }
        return tmpEvents;
    }

    public String getJson() {
        return json;
    }

    public String[] getEventArray() {
        return eventStr;
    }

    public String getEvent() {
        StringBuilder st = new StringBuilder();
        for (String anEventStr : eventStr) {
            if (anEventStr != null && anEventStr.length() > 0) {
                st.append(anEventStr);
                st.append(".");
            } else break;
        }
        return StaticFunction.ensureLength(st.toString(), eventLength);
    }

    public String getDate() {
//        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        Date date = new Date(timestamp);
        return Common.df.format(date);
    }


    public long getTimestamp() {
        return timestamp;
    }

    public Long getValue() {
        return new Long(value);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(seqUid);
        stringBuilder.append("\t");
        stringBuilder.append(getEvent());
        stringBuilder.append("\t");
        stringBuilder.append(value);
        stringBuilder.append("\t");
        stringBuilder.append(timestamp);
        return stringBuilder.toString();
    }


    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Long getSeqUid() {
        return seqUid;
    }

    public void setSeqUid(long seqUid) {
        this.seqUid = seqUid;

    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }


}