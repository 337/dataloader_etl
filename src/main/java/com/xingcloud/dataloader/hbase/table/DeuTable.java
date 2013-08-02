package com.xingcloud.dataloader.hbase.table;

import com.xingcloud.dataloader.StaticConfig;
import com.xingcloud.dataloader.lib.Event;
import com.xingcloud.dataloader.lib.SeqUidCacheMap;
import com.xingcloud.dataloader.lib.User;
import com.xingcloud.util.Constants;
import com.xingcloud.util.RandSpeUidMd5;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;

import java.util.*;

/**
 * put操作缓存。
 * 将所有addEvent()添加进来的event转化为Put操作，放在putCache里面，然后提供getCache()提供最后的结果。
 * <p/>
 * 这里会拼出rowkey。
 * Hbase事件的表结构
 * Rowkey:date+event+encodeBase64(uid)
 * CF:val
 * Qualifier:val
 * Value
 * Timestamp
 * <p/>
 * Rowkey 是日期，事件名称和base64转换后的uid的拼装
 * CF和Qualifier都是定值
 * value和timestamp都是一个事件一对（每个表默认的version上限是300，所以一个用户同一天发生的同一个事件只能有300个）
 * <p/>
 * Author: qiujiawei
 * Date:   12-4-9
 */
public class DeuTable implements MainTable {
    private Random random = null;
    public static final Log LOG = LogFactory.getLog(DeuTable.class);

    public static final String columnFamily = "val";
    private static final int maxVersion = Constants.HBASE_VERSION;

    private static final String tableFistName = "deu_";

    private static final String tableFixLastName = "_deu";

    private String project = null;
    private String date;


    public DeuTable(String project, String date) {
        this.project = project;
        this.date = date;
        random = new Random();
    }

    private Map<byte[], List<Event>> eventCache = new HashMap<byte[], List<Event>>();

    private Map<byte[], List<Event>> delayCache = new HashMap<byte[], List<Event>>();

    public String getTableName(String projectIdentifier) {
        return tableFistName + projectIdentifier;
    }

    public String getFixTableName(String projectIdentifier) {
        return projectIdentifier + tableFixLastName;
    }


    public void addEvent(Event event) {
        byte[] rowKey = null;
        try {
            rowKey = generateRowKeyFor(event);
        } catch (Exception e) {
            LOG.error("generateRowKeyFor ", e);
        }
        if (rowKey == null) {
            return;
        }
        toCache(rowKey, event);

    }

    private void toCache(byte[] rowKey, Event newEvent) {
        newEvent.setRowKey(rowKey);

        if (newEvent.getDate().compareTo(this.date) < 0) {
            List<Event> delayEvents = delayCache.get(rowKey);
            if (delayEvents == null) {
                delayEvents = new ArrayList<Event>();
                delayCache.put(rowKey, delayEvents);
            }
            delayEvents.add(newEvent);
        }
        List<Event> events = eventCache.get(rowKey);
        if (events == null) {
            events = new ArrayList<Event>();
            eventCache.put(rowKey, events);
        }
        events.add(newEvent);
    }

    /**
     * 为event生成hash uid和rowkey。
     *
     * @param event event
     * @return 生成的rowkey
     */
    private byte[] generateRowKeyFor(Event event) throws Exception {
        String eventName = event.getEvent();
        if (eventName == null || eventName.length() <= 0) {
            return null;
        }

        //忽略 update.*: 更新用户属性的操作在用户事件中不处理。
        if (event.getEvent().equals("update.")) {
            return null;
        }

        byte[] rowKey = null;
        //如果为random的单词，就随机一个字符串
        if (User.isSpecialUid(event.getUid())) {
            int md5RandomUid = RandSpeUidMd5.md5(random.nextInt(Constants.RANDOMADUIDRANGE));
            event.setUid(String.valueOf(md5RandomUid));
            event.setSeqUid(md5RandomUid);
        } else {
            Long seqUid = event.getSeqUid();
            if (seqUid == null) {
                seqUid = (long) SeqUidCacheMap.getInstance().getUidCache(project, event.getUid());
                event.setSeqUid(seqUid);
            }
        }

        long samplingUid = UidMappingUtil.getInstance().decorateWithMD5(event.getSeqUid());
        rowKey = UidMappingUtil.getInstance().getRowKeyV2(event.getDate(), event.getEvent(), samplingUid);
        event.setRowKey(rowKey);
        return rowKey;
    }

    public Map<byte[], List<Event>> getPutEvents() {
        return eventCache;
    }

    public Map<byte[], List<Event>> getDelayEvents() {
        return delayCache;
    }

    public HTableDescriptor getHTableDescriptor(String project) {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(getTableName(project));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        hColumnDescriptor.setMaxVersions(maxVersion);
        hColumnDescriptor.setBlocksize(512 * 1024);
        if (StaticConfig.compress) hColumnDescriptor.setCompressionType(Compression.Algorithm.LZO);
        hTableDescriptor.addFamily(hColumnDescriptor);
        return hTableDescriptor;
    }

    public HTableDescriptor getFixHTableDescriptor(String project) {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(getFixTableName(project));
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
        hColumnDescriptor.setMaxVersions(maxVersion);
        hColumnDescriptor.setBlocksize(512 * 1024);
        if (StaticConfig.compress) hColumnDescriptor.setCompressionType(Compression.Algorithm.LZO);
        hTableDescriptor.addFamily(hColumnDescriptor);
        return hTableDescriptor;
    }


}
