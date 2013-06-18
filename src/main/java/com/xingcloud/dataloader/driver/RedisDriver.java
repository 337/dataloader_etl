package com.xingcloud.dataloader.driver;

import com.xingcloud.dataloader.dao.MessageQueue;
import com.xingcloud.redis.RedisResourceManager;
import com.xingcloud.redis.RedisShardedPoolResourceManager;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;

/**
 * 用于情况redis 某个db的内容
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class RedisDriver {
    static  Jedis[] jedisList =new Jedis[20];
    static void flushdb(int db){
         Jedis instance=RedisResourceManager.getInstance().getCache();
         instance.select(db);
         instance.flushDB();
    }

	@Deprecated
    static public void main(String[] args){
        RedisDriver.flushdb(Integer.valueOf(args[0]));
    }

}
