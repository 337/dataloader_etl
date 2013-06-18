package com.xingcloud.dataloader.hbase.hash;

import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * 通过configPath指定的HBase配置信息，和hashutil指定的节点和数据分布信息，
 * 提供多个HBase shard的Configuration的信息
 * Author: mulisen
 * Date:   8/22/12
 */
public class HBaseHash {

	private UidMappingUtil uidMappingUtil = UidMappingUtil.getInstance();

	private String configPath = null;

	private Map<String, Configuration> HBases = new HashMap<String, Configuration>();

	HBaseHash(String configPath) throws IOException {
		this.configPath = configPath;
		initConfig();
	}

	HBaseHash(Map<String, Configuration> configMap, UidMappingUtil uidMappingUtil){
		this.uidMappingUtil = uidMappingUtil;
		this.HBases = configMap;
	}

	private void initConfig() throws IOException {
        uidMappingUtil.nodes();

		InputStream input = getClass().getClassLoader().getResourceAsStream(configPath);
		Yaml yaml = new Yaml();
		Map data = (Map) yaml.load(input);
		ArrayList zookeeperList = (ArrayList) data.get("zookeepers");
		for (int i = 0; i < zookeeperList.size(); i++) {
			Map zookeeper = (Map) zookeeperList.get(i);
			String name = (String) zookeeper.get("name");
			String port = String.valueOf(zookeeper.get("port"));
			Configuration conf = new Configuration();
			conf.set("hbase.zookeeper.quorum", name);
			conf.set("hbase.zookeeper.property.clientPort", port);

			/*Default conf*/
			conf.set("hbase.client.ipc.pool.type", "RoundRobinPool");
			conf.set("hbase.client.ipc.pool.size", "10");
			conf.set("hbase.rpc.timeout", "3600000");
			Configuration hcf = HBaseConfiguration.create(conf);
			HBases.put(name, hcf);
		}
	}

	public Configuration hash(long seqId) {
		if(uidMappingUtil==null){
			return HBases.get("");
		}
		return HBases.get(uidMappingUtil.hash(seqId));
	}

	public Map<String, Configuration> configs(){
		return HBases;
	}

	public static void main(String[] args) throws IOException {
		new HBaseHash(null);
	}
}
