package com.xingcloud.redis;

import com.xingcloud.util.config.ConfigReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisResourceManager {
    private static Log logger = LogFactory.getLog(RedisResourceManager.class);
    
    private String host;
    private int port;
    private int timeout;
    
    private static JedisPool pool;
    
    private RedisResourceManager() {
        init();
    }
    
    private void init() {
        if (pool == null) {
            host = ConfigReader.getConfig("Config.xml", "redis", "host");
            port = Integer.parseInt(ConfigReader.getConfig("Config.xml", "redis", "port"));
            timeout = Integer.parseInt(ConfigReader.getConfig("Config.xml", "redis", "timeout"));
            
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxActive(Integer.parseInt(ConfigReader.getConfig("Config.xml", "redis", "max_active")));
            config.setMaxIdle(Integer.parseInt(ConfigReader.getConfig("Config.xml", "redis", "max_idle")));
            config.setMaxWait(Integer.parseInt(ConfigReader.getConfig("Config.xml", "redis", "max_wait")));
            
            logger.info("Init Redis: ");
            logger.info("Host: " + host);
            logger.info("Max active: " + config.maxActive);
            logger.info("Max idle: " + config.maxIdle);
            logger.info("Max wait: " + config.maxWait);
            logger.info("Time out: " + timeout);
            pool = new JedisPool(config, host, port, timeout);
        }
    }
    
    public synchronized static RedisResourceManager getInstance() {
        return InnerHolder.INSTANCE;
    }
    
    public JedisPool getRedisPool() {
        return pool;
    }
    
    private static class InnerHolder {
        static final RedisResourceManager INSTANCE = new RedisResourceManager();
    }
    
    public Jedis getCache() {
    	Jedis jedis = pool.getResource();
        jedis.select(0);
    	return jedis;

    }
    
    public void returnResource(Jedis jedis) {
        pool.returnResource(jedis);
    }

    public void returnBrokenResource(Jedis jedis) {
       pool.returnBrokenResource(jedis);
    }

    public void destory() {
        if (pool != null) {
           pool.destroy();
        }
    }
    
    
}
