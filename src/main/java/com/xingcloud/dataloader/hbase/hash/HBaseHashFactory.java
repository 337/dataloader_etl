package com.xingcloud.dataloader.hbase.hash;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;

/**
 * Author: mulisen
 * Date:   8/22/12
 */
public class HBaseHashFactory {
	public static HBaseHash newHBaseHash(String resourcePath) throws IOException {
		String fileName = resourcePath;
		if(fileName.endsWith(".xml")){
			//普通HBase配置，返回只有一个Configuration的HBaseHash
			Configuration config = new Configuration();
			config.addResource("hbase-default.xml");
			config.addResource(fileName);
			HashMap<String, Configuration> oneConfig = new HashMap<String, Configuration>();
			oneConfig.put("",config);
			HBaseHash hash = new HBaseHash(oneConfig, null);
			return hash;
		}else if(fileName.endsWith(".yaml")){
			//多HBase配置。
			HBaseHash hash = new HBaseHash(fileName);
			return hash;
		}
		return null;
	}
}
