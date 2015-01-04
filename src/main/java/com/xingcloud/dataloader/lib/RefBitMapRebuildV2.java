package com.xingcloud.dataloader.lib;

import com.xingcloud.dataloader.hbase.hash.HBaseHash;
import com.xingcloud.dataloader.hbase.hash.HBaseKeychain;
import com.xingcloud.dataloader.hbase.table.user.UserPropertyBitmaps;
import com.xingcloud.hbase.HBaseOperation;
import com.xingcloud.redis.RedisShardedPoolResourceManager;
import com.xingcloud.util.Common;
import com.xingcloud.util.Constants;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import com.xingcloud.mysql.MySqlDict;

import java.io.*;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.*;

/**
 * User: IvyTang
 * Date: 13-7-15
 * Time: PM3:47
 */
public class RefBitMapRebuildV2 {

  private static final Log LOG = LogFactory.getLog(RefBitMapRebuildV2.class);

    public static byte[] columnfamily = Bytes.toBytes(Constants.userColumnFamily);
    public static byte[] qualifier = Bytes.toBytes(Constants.userColumnFamily);


  private static final RefBitMapRebuildV2 instance = new RefBitMapRebuildV2();

  private Set<String> ignoreProjects = new HashSet<String>();

  private String[] canRecoverFromLocalFiles = new String[]{User.refField, User.registerField, User.geoipField, User.nationField};

  private RefBitMapRebuildV2() {
    ignoreProjects.add("govome");
    ignoreProjects.add("globososo");
  }

  public static RefBitMapRebuildV2 getInstance() {
    return instance;
  }

  public void rebuildSixtyDays(String project, String date, int index) throws Exception {
    if (ignoreProjects.contains(project)) return;
    if (index == Constants.FIRST_TASK_NUM) {
      rebuildSixtyDaysActiveUsersFromMySQL(project, date);
    } else {
      if (UserPropertyBitmaps.getInstance().ifPropertyNull(project, User.refField)) {
        rebuildSixtyDaysActiveUsersFromLocalFile(project, canRecoverFromLocalFiles);
      }
    }
  }

  private void rebuildSixtyDaysActiveUsersFromMySQL(String project, String date) throws Exception {
    dumpSixtyDaysActiveUsersToLocal(project, date);
    rebuildSixtyDaysActiveUsersFromLocalFile(project, new String[]{User.refField});
  }

  private void dumpSixtyDaysActiveUsersToLocal1(String project, String date) {
    // load 60 active users from mysql to localfile.
    long currentTime = System.currentTimeMillis();
    String[] nodes = UidMappingUtil.getInstance().nodes().toArray(new String[UidMappingUtil.getInstance().nodes()
            .size()]);
//    String[] nodes = new String[]{"192.168.1.147","192.168.1.148","192.168.1.150","192.168.1.152","192.168.1.154",
//            "192.168.1.151"};
    shuffle(nodes);
    for (String mysqlHost : nodes) {
      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS + File.separator + project + "_" + mysqlHost;
      BufferedReader dumpBufferedReader = null;
      try {
        Runtime rt = Runtime.getRuntime();
        String sqlCMD = "mysql -uxingyun -pOhth3cha --database "+getMySQLDBName(project) + " -h" + mysqlHost + " -ss -e\"SELECT" +
                " uid FROM last_login_time where val>=" + TimeIndexV2.getSixtyDaysBefore(date+"000000") + " and val<" +
                TimeIndexV2.getRightHoursBefore(date + "000000") + "\" > " + filePath;
        LOG.info(sqlCMD);
        String[] cmds = new String[]{"/bin/sh", "-c", sqlCMD};
        Process process = rt.exec(cmds);
        dumpBufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String cmdOutput = null;
        while ((cmdOutput = dumpBufferedReader.readLine()) != null)
          LOG.info(cmdOutput);
        int result = process.waitFor();
        if (result != 0)
          throw new RuntimeException("ERROR !!!! get 60 days active user for " + project + " " + mysqlHost + " failed.");
      } catch (Exception e) {
        LOG.error(e.getMessage());
      } finally {
        if (dumpBufferedReader != null)
          try {
            dumpBufferedReader.close();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
      }
    }
    LOG.info(project + " dump  60 active from mysql  using " + (System.currentTimeMillis() - currentTime) + " " +
            "ms.");
  }

    private synchronized void dumpSixtyDaysActiveUsersToLocal(String project, String date) throws Exception {
        // load 60 active users from hbase to localfile.
        long currentTime = System.currentTimeMillis();
        String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS2 + File.separator + project;
        File file = new File(filePath);

//        Configuration conf = HBaseConfiguration.create();
        HBaseKeychain keychain = HBaseKeychain.getInstance();
        List<HBaseHash> hashes = keychain.getConfigs();
        for (int i = 0; i < hashes.size(); i++) {
            HBaseHash hash = hashes.get(i);
            Map<String, Configuration> configs = hash.configs();
            for (Map.Entry<String, Configuration> entry : configs.entrySet()) {
                Configuration value = entry.getValue();
                HTable table = new HTable(value, Constants.ATTRIBUTE_TABLE);

                String sixtyDaysBefore = TimeIndexV2.getSixtyDaysBefore(date+"000000");
                String rightHoursBefore = TimeIndexV2.getRightHoursBefore(date + "000000");
                int pidDict = Constants.dict.getPidDict(project);
                int attrDict = Constants.dict.getAttributeDict(Constants.LAST_LOGIN_TIME);
                byte[] startKey = Bytes.add(Bytes.toBytes(pidDict), Bytes.toBytes(attrDict));
                byte[] endKey = Bytes.add(Bytes.toBytes(pidDict), Bytes.toBytes(attrDict + 1));

                Scan scan = new Scan();
                scan.setStartRow(startKey);
                scan.setStopRow(endKey);
                scan.addColumn(Bytes.toBytes(Constants.userColumnFamily), Bytes.toBytes(Constants.userColumnQualifier));
                scan.setCaching(10000);

                FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                Filter filter1 = new SingleColumnValueFilter(columnfamily, qualifier, CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(Long.parseLong(sixtyDaysBefore))));
                Filter filter2 = new SingleColumnValueFilter(columnfamily, qualifier, CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes(Long.parseLong(rightHoursBefore))));
                list.addFilter(filter1);
                list.addFilter(filter2);
                scan.setFilter(list);

                ResultScanner scanner = table.getScanner(scan);

                BufferedWriter bw = new BufferedWriter(new FileWriter(file, true));
                for(Result r : scanner){
                    byte[] rowkey = r.getRow();
                    long uid = Bytes.toLong(Bytes.tail(rowkey, 8));
                    bw.write(String.valueOf(uid));
                }
            }
        }

        LOG.info(project + " dump  60 active from mysql  using " + (System.currentTimeMillis() - currentTime) + " ms.");

    }

  private void rebuildSixtyDaysActiveUsersFromLocalFile(String project, String[] properties) {

    for (String property : properties){
      UserPropertyBitmaps.getInstance().resetPropertyMap(project,property);
      UserPropertyBitmaps.getInstance().initPropertyMap(project, property);
    }

    long currentTime = System.currentTimeMillis();

      String filePath = Constants.SIXTY_DAYS_ACTIVE_USERS2 + File.separator + project;
      BufferedReader bufferedReader = null;
      try {
        bufferedReader = new BufferedReader(new FileReader(filePath));
        String tmpLine = null;
        while ((tmpLine = bufferedReader.readLine()) != null) {
          long innerUid = Long.parseLong(tmpLine) & 0xffffffffl;
          for (String property : properties)
            UserPropertyBitmaps.getInstance().markPropertyHit(project, innerUid, property);
        }
      } catch (FileNotFoundException e) {
        //do thing
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
      } finally {
        if (bufferedReader != null)
          try {
            bufferedReader.close();
          } catch (IOException e) {
            LOG.error(e.getMessage(), e);
          }
      }
      LOG.info(project+" read "+filePath);

    LOG.info(project + " rebuild ref,nation,register_time,geoip from local file using " + (System.currentTimeMillis()
            - currentTime) + " " + "" + "ms.");
  }


  private void exch(String[] list, int i, int j) {
    String swap = list[i];
    list[i] = list[j];
    list[j] = swap;
  }

  // take as input an array of strings and rearrange them in random order
  private void shuffle(String[] list) {
    int N = list.length;
    Random random = new Random();
    for (int i = 0; i < N; i++) {
      int r = random.nextInt(N); // between i and N-1
//      System.out.println(r);
      exch(list, i, r);
    }

  }

  private static String getMySQLDBName(String project){
    return "16_"+project;
  }


  public static void main(String[] args) throws Exception {

    String project = args[0];
    String date = args[1];

      RefBitMapRebuildV2 rb = new RefBitMapRebuildV2();
      rb.dumpSixtyDaysActiveUsersToLocal(project, date);

  }
}



