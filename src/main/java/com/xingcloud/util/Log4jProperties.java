package com.xingcloud.util;

import org.apache.log4j.PropertyConfigurator;

import java.io.InputStream;
import java.util.Properties;

/**
 * how to use:
 * 1.add next code into your main class
 * Log4jProperties.init();
 *
 * 2.add follow code into your class
 * public static final Log LOG = LogFactory.getLog(MapReduceJob.class);
 *
 * 3.use LOG.error() LOG.info() LOG.debug() to log
 *
 * 4.config the log4j.properties
 */
public class Log4jProperties {
	static private Properties properties=new Properties();
	static public void init(){
//		loadProperties("log4j.properties");
        loadProperties("xingcloudlog4j.properties");
        PropertyConfigurator.configure(Log4jProperties.getProperties());
	}

	private static void loadProperties(String fileName) {
		try{
			Properties temp=new Properties();
			InputStream is=Log4jProperties.class.getClassLoader().getResourceAsStream(fileName);
			temp.load(is);
	
			for(Object key:temp.keySet()){
				properties.setProperty((String)key,temp.getProperty((String)key) );
			}
		}
		catch(Exception e){
		    System.out.println("inti failed"+e);
		}
	}
	private static Properties getProperties() {
		return properties;
	}

}