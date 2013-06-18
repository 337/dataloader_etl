package com.xingcloud.dataloader.hbase.hash;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 为了支持多个hbase集群，读取配置文件，初始化多个Configuration对象。
 *
 * 至少有一个config，对应hbase-site.xml 或者 hbase-site.yaml；
 * 如果除此之外，还有 hbase-site.1.xml(.yaml), hbase-site.2.xml(.yaml), ....
 * 那么建立相对应的config对象。
 * 从hbase-site.1.xml往后找，直到找不到文件为止。
 * Author: mulisen
 * Date:   8/20/12
 */
public class HBaseKeychain {
	public static final Log LOG = LogFactory.getLog(HBaseKeychain.class);
	private List<HBaseHash> configs = new ArrayList<HBaseHash>();

	private static HBaseKeychain instance = new HBaseKeychain();

	private HBaseKeychain(){
		try {
			init();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void init() throws IOException {
		//default config from hbase-site.xml
		String path = "hbase-site.xml";

		URL file = getClass().getClassLoader().getResource(path);
		if(file == null){
			path = "hbase-site.yaml";
			file = getClass().getClassLoader().getResource(path);
		}

		configs.add(HBaseHashFactory.newHBaseHash(path));

//		URL file = getClass().getClassLoader()
		for(int i=1;;i++){
			path = "hbase-site."+i+".xml";
			file = getClass().getClassLoader().getResource(path);
			if(file == null){
				path = "hbase-site."+i+".yaml";
				file = getClass().getClassLoader().getResource(path);
			}
			if(file == null){
				break;
			}
			configs.add(HBaseHashFactory.newHBaseHash(path));
		}
	}

	public static HBaseKeychain getInstance() {
		return instance;
	}

	public List<HBaseHash> getConfigs() {
		return configs;
	}
}
