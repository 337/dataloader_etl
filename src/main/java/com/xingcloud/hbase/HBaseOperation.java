package com.xingcloud.hbase;

import com.xingcloud.dataloader.hbase.hash.HBaseHash;
import com.xingcloud.dataloader.hbase.hash.HBaseKeychain;
import com.xingcloud.util.Constants;
import com.xingcloud.util.manager.DateManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class HBaseOperation {
    private static Configuration conf = null;
    private static Log logger = LogFactory.getLog(HBaseOperation.class);
    private Map<String, Set<String>> eventTypesMap = new ConcurrentHashMap<String, Set<String>>();

    private static HBaseOperation m_instance;

    static {
        conf = HBaseConfiguration.create();
    }

    private HBaseOperation() {

    }

    public synchronized static HBaseOperation getInstance() {
        if (m_instance == null) {
            m_instance = new HBaseOperation();
        }
        return m_instance;
    }

    public void createTable(String tableName, String[] cfs) throws IOException {
        createTable(tableName, cfs, false, 3, null);
    }

    public void createTable(String tableName, String[] cfs, boolean compress,
                            int maxVersion, String coprocessorName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("Table already existed!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            if (coprocessorName != null) tableDesc.addCoprocessor(coprocessorName);
            for (int i = 0; i < cfs.length; i++) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(
                        cfs[i]);

                if (compress)
                    hColumnDescriptor
                            .setCompressionType(Compression.Algorithm.LZO);
                hColumnDescriptor.setMaxVersions(maxVersion);
                tableDesc.addFamily(hColumnDescriptor);
            }
            admin.createTable(tableDesc);
            logger.info("Create table finished!");
        }
    }

    public void ensureTable(String tableName, HTableDescriptor hTableDescriptor)
            throws IOException {
        HBaseKeychain keychain = HBaseKeychain.getInstance();
        List<HBaseHash> hashes = keychain.getConfigs();
        for (int i = 0; i < hashes.size(); i++) {
            HBaseHash hash = hashes.get(i);
            Map<String, Configuration> configs = hash.configs();
            for (Map.Entry<String, Configuration> entry : configs.entrySet()) {
                String key = entry.getKey();
                Configuration value = entry.getValue();
                ensureTable(tableName, hTableDescriptor, value);
            }
        }
    }

    private void ensureTable(String tableName, HTableDescriptor hTableDescriptor, Configuration configuration) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(configuration);
        if (admin.tableExists(tableName)) {
            System.out.println(tableName + "Table already existed!");
        } else {
            admin.createTable(hTableDescriptor);
            logger.info("Create table:" + tableName + " finished! ");
        }
    }

    public void checkTable(String tableName)
            throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("Table already existed!");
            System.out.println("TableDescriptor is :" + admin.getTableDescriptor(tableName.getBytes()));

        } else {
            System.out.println("Table isn't existed...");
        }
    }

    public void deleteTable(String tableName) throws IOException {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (admin.isTableAvailable(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                logger.info("Table deleted sucessfully!");
            } else
                logger.info("Table is not exist.");
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    public void writeRow(HTable table, String rowKey, String columnFamily,
                         String qualifier, int value) {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeRow(HTable table, String rowKey, String family,
                         String qualifier, String value) {
        try {
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void deleteRow(HTable table, String tableName, String rowKey)
            throws IOException {
        List<Delete> list = new ArrayList<Delete>();
        Delete d1 = new Delete(rowKey.getBytes());
        list.add(d1);
        table.delete(list);
        logger.info("Delete successful!");
    }

    public void selectRow(HTable table, String rowKey) throws IOException {
        Get g = new Get(rowKey.getBytes());
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            printRow(kv);
        }
    }

    public void selectRow(HTable table, String rowKey, long minStamp,
                          long maxStamp) throws IOException {
        Get g = new Get(rowKey.getBytes());
        g.setTimeRange(minStamp, maxStamp);
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            printRow(kv);
        }
    }

    public void selectRow(HTable table, String rowKey, String family)
            throws IOException {
        Get g = new Get(rowKey.getBytes());
        g.addFamily(family.getBytes());
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            printRow(kv);
        }
    }

    public void selectRow(HTable table, String rowKey, String family,
                          long minStamp, long maxStamp) throws IOException {
        Get g = new Get(rowKey.getBytes());
        g.addFamily(family.getBytes());
        g.setTimeRange(minStamp, maxStamp);
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            printRow(kv);
        }
    }

    public void selectRow(HTable table, String rowKey, String family,
                          String qualifier) throws IOException {
        Get g = new Get(rowKey.getBytes());
        g.addColumn(family.getBytes(), qualifier.getBytes());
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            printRow(kv);
        }
    }

    public void selectRow(HTable table, String rowKey, String family,
                          String qualifier, long minStamp, long maxStamp) throws IOException {
        Get g = new Get(rowKey.getBytes());
        g.addColumn(family.getBytes(), qualifier.getBytes());
        g.setTimeRange(minStamp, maxStamp);
        Result rs = table.get(g);
        for (KeyValue kv : rs.raw()) {
            printRow(kv);
        }
    }

    private void printRow(KeyValue kv) {
        System.out.print(new String(kv.getRow() + " "));
        System.out.print(new String(kv.getFamily() + ":"));
        System.out.print(new String(kv.getQualifier()));
        System.out.print(kv.getTimestamp() + " ");
        System.out.println(new String(kv.getValue()));
    }

    public void scaner(String tableName) {
        try {
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner rs = table.getScanner(s);
            for (Result r : rs) {
                for (KeyValue kv : r.raw()) {
                    printRow(kv);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long getEventNum(String projectId, List<String> uidList,
                            String startDate, String endDate, String eventFilter)
            throws IOException {
        Collections.sort(uidList);
        Long startTime = System.currentTimeMillis();

        String startUid = null;
        String endUid = null;
        if (uidList.size() > 2) {
            startUid = uidList.get(0);
            endUid = uidList.get(uidList.size() - 1);
        }

        List<Result> results = new ArrayList<Result>();
        if (!isNeedScanner(eventFilter)) {

            results = getRowWithQuery(projectId, eventFilter, startUid, endUid,
                    startDate, endDate);
        } else {
            results = getRowsWithScanner(projectId, eventFilter, startUid,
                    endUid, startDate, endDate);
        }

        Map<String, Integer> uidMap = new HashMap<String, Integer>();
        for (String uid : uidList) {
            uidMap.put(uid, 1);
        }

        long counter = 0;
        for (Result r : results) {
            for (KeyValue kv : r.raw()) {
                String uid = new String(kv.getQualifier());
                if (uidMap.containsKey(uid) || uidMap.size() == 0) {
                    counter++;
                }
            }
        }

        logger.debug("scan rows use "
                + (System.currentTimeMillis() - startTime) / 1000 + "seconds");
        return counter;
    }

    public long getEventAmount(String projectId, List<String> uidList,
                               String startDate, String endDate, String eventFilter)
            throws IOException {
        Collections.sort(uidList);
        Long startTime = System.currentTimeMillis();

        List<Result> results = new ArrayList<Result>();
        if (!isNeedScanner(eventFilter)) {
            results = getRowWithQuery(projectId, eventFilter, uidList.get(0),
                    uidList.get(uidList.size() - 1), startDate, endDate);
        } else {
            results = getRowsWithScanner(projectId, eventFilter,
                    uidList.get(0), uidList.get(uidList.size() - 1), startDate,
                    endDate);
        }

        Map<String, Integer> uidMap = new HashMap<String, Integer>();
        for (String uid : uidList) {
            uidMap.put(uid, 1);
        }

        long counter = 0;
        for (Result r : results) {
            for (KeyValue kv : r.raw()) {
                String uid = new String(kv.getQualifier());
                String val = new String(kv.getValue());
                if (uidMap.containsKey(uid) || uidMap.size() == 0) {
                    counter += Long.parseLong(val);
                }
            }
        }
        logger.debug("scan rows use "
                + (System.currentTimeMillis() - startTime) / 1000 + "seconds");
        return counter;
    }

    private List<Result> getRowWithQuery(String projectId, String eventFilter,
                                         String startUid, String endUid, String startDate, String endDate)
            throws IOException {
        HTable htable = new HTable(conf, projectId);
        ColumnRangeFilter columnRangeFilter = null;
        if (startUid != null && endUid != null) {
            columnRangeFilter = new ColumnRangeFilter(startUid.getBytes(),
                    true, endUid.getBytes(), true);
        }
        List<String> dateRange = getDateRange(startDate, endDate);
        Set<String> candidateEvents = applyEventFilter(eventFilter, projectId);
        List<Get> gets = new ArrayList<Get>();
        for (String date : dateRange) {
            for (String event : candidateEvents) {
                System.out.println("row key: " + date + event);
                Get get = new Get((date + event).getBytes());
                if (columnRangeFilter != null) {
                    get.setFilter(columnRangeFilter);
                }
                gets.add(get);
            }
        }

        List<Result> results = Arrays.asList(htable.get(gets));
        htable.close();
        return results;
    }

    private List<Result> getRowsWithScanner(String projectId,
                                            String eventFilter, String startUid, String endUid,
                                            String startDate, String endDate) throws IOException {
        List<Result> results = new ArrayList<Result>();

        HTable htable = new HTable(conf, projectId);
        ResultScanner rs = null;
        ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(
                startUid.getBytes(), true, endUid.getBytes(), true);
        List<String> dateRange = getDateRange(startDate, endDate);

        eventFilter = eventFilter.replace(".*", "");
        String eventEnd = getNextEvent(eventFilter);
        for (String date : dateRange) {
            try {
                logger.debug(date + eventEnd);
                Scan scan = new Scan();
                scan.addFamily("event".getBytes());
                scan.setStartRow((date + eventFilter).getBytes());
                scan.setStopRow((date + eventEnd).getBytes());
                scan.setFilter(columnRangeFilter);
                scan.setCaching(10000);
                rs = htable.getScanner(scan);
                for (Result r = rs.next(); r != null; r = rs.next()) {
                    results.add(r);
                }
            } finally {
                rs.close();
            }
        }
        htable.close();
        return results;
    }

    private List<String> getDateRange(String startDate, String endDate) {

        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        df.setTimeZone(Constants.TMZ);
        /**
         * from start date to end date
         */
        List<String> dateRange = new ArrayList<String>();
        for (Long startTime = DateManager.getTimestamp(startDate); startTime <= DateManager
                .getTimestamp(endDate); startTime += 1000 * 3600 * 24) {
            dateRange.add(df.format(new Date(startTime)));
        }
        return dateRange;
    }

    private boolean isNeedScanner(String eventFilter) {
        if (eventFilter.indexOf('*') != eventFilter.length() - 1) {
            /**
             * if * is not only at the last position, use regular expression
             */
            return false;
        } else {
            /**
             * else if * is at the last position, just a range scan
             */
            return true;
        }
    }

    public Set<String> getUidSet(String projectId, String startUid,
                                 String endUid, String startDate, String endDate, String eventFilter)
            throws IOException {
        Set<String> user = new HashSet<String>();

        Long startTime = System.currentTimeMillis();

        List<Result> results = new ArrayList<Result>();
        if (!isNeedScanner(eventFilter)) {
            results = getRowWithQuery(projectId, eventFilter, startUid, endUid,
                    startDate, endDate);
        } else {
            results = getRowsWithScanner(projectId, eventFilter, startUid,
                    endUid, startDate, endDate);
        }

        for (Result r : results) {
            if (!r.isEmpty()) {
                for (byte[] userId : r.getFamilyMap("event".getBytes())
                        .keySet()) {
                    user.add(new String(userId));
                }
            }
        }

        logger.debug("scan rows use "
                + (System.currentTimeMillis() - startTime) / 1000 + "seconds");

        return user;
    }

    private Set<String> applyEventFilter(String eventFilter, String projectId)
            throws IOException {
        Long startTime = System.currentTimeMillis();
        Set<String> candidateEvents = new HashSet<String>();
        if (eventFilter == null || eventFilter.isEmpty()) {
            return candidateEvents;
        }
        Pattern pattern = Pattern.compile(eventFilter.replace(".", "\\.")
                .replace("*", ".*"));
        for (String eventType : getEventType(projectId)) {
            if (pattern.matcher(eventType).matches()) {
                System.out.println("Match event: " + eventType);
                candidateEvents.add(eventType);
            }
        }
        logger.debug("scan event type use "
                + (System.currentTimeMillis() - startTime) / 1000 + "seconds");
        return candidateEvents;
    }

    private String getNextEvent(String eventFilter) {
        StringBuilder endEvent = new StringBuilder(eventFilter);
        endEvent.setCharAt(eventFilter.length() - 1,
                (char) (endEvent.charAt(eventFilter.length() - 1) + 1));
        return endEvent.toString();
    }

    public Set<String> getEventType(String projectId) throws IOException {
        Set<String> eventTypes = new HashSet<String>();
        HTable htable = new HTable(conf, projectId + "_meta");
        Scan scan = new Scan();
        scan.addColumn("event".getBytes(), "type".getBytes());
        ResultScanner rs = htable.getScanner(scan);
        for (Result r = rs.next(); r != null; r = rs.next()) {
            eventTypes.add(new String(r.getRow()));
        }
        htable.close();
        System.out.println("Total event types: " + eventTypes.size());

        return eventTypes;
    }

    public Set<String> getEventType(String projectId, int idx, int pageSize)
            throws IOException {
        Set<String> eventTypes = new TreeSet<String>();
        HTable htable = new HTable(conf, projectId + "_meta");
        Scan scan = new Scan();
        scan.addColumn("event".getBytes(), "type".getBytes());
        ResultScanner rs = htable.getScanner(scan);
        int counter = 0;
        int min = idx * pageSize;
        int max = (idx + 1) * pageSize;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            counter++;
            if (counter > min && counter <= max) {
                eventTypes.add(new String(r.getRow()));
            }
            if (counter > max) {
                break;
            }
        }
        htable.close();
        System.out.println("Total event types: " + eventTypes.size());

        return eventTypes;
    }
}
