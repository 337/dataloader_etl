package com.xingcloud.dataloader.tools;

import com.xingcloud.redis.RedisResourceManager;
import com.xingcloud.util.Constants;
import redis.clients.jedis.Jedis;


public class ClearTaskQueue {

    public static void main(String[] args) {

        Jedis jedis = RedisResourceManager.getInstance().getCache();
        jedis.select(Constants.REDIS_DB_NUM);

        for (String arg : args) {
            long result = jedis.del(arg);
            System.out.println(arg + " delete task : " + result);
        }

    }

}
