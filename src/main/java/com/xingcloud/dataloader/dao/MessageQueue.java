package com.xingcloud.dataloader.dao;

import com.xingcloud.dataloader.server.message.DataLoaderMessage;
import com.xingcloud.dataloader.server.message.MessageFactory;
import com.xingcloud.dataloader.server.message.NullMessage;
import com.xingcloud.redis.RedisResourceManager;
import com.xingcloud.util.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Dataloader Task Queue. backed by redis
 * Author: qiujiawei
 * Date:   12-5-16
 */
public class MessageQueue {
    public Log LOG = LogFactory.getLog(MessageQueue.class);
    static final int dbNumber = Constants.REDIS_DB_NUM;
    private String ip;

    public MessageQueue(String ip) {
        this.ip = ip;
    }

    private String getNextEvent(Jedis jedis) {
        List<String> re = jedis.blpop(100000, ip);
        return re.get(1);
    }

    public DataLoaderMessage getNextMassage() {
        Jedis jedis = null;
        try {
            jedis = initJedis();
            return MessageFactory.getMessage(getNextEvent(jedis));
        } catch (Exception e) {
            LOG.error("get next message catch exception", e);
            if (jedis != null) {
                RedisResourceManager.getInstance().returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                RedisResourceManager.getInstance().returnResource(jedis);
            }
        }
        return new NullMessage();
    }

    public boolean addMessageFirst(DataLoaderMessage message) {
        boolean returnResult = false;
        for (int i = 0; i < 3; i++) {
            Jedis jedis = null;
            try {
                jedis = initJedis();
                jedis.lpush(ip, message.toString());
                returnResult = true;
                break;
            } catch (Exception e) {
                if (jedis != null) {
                    RedisResourceManager.getInstance().returnBrokenResource(jedis);
                    jedis = null;
                }
                if (i >= 2) {
                    LOG.error("addMessageFirst catch exception", e);
                }
            } finally {
                if (jedis != null) {
                    RedisResourceManager.getInstance().returnResource(jedis);
                }
            }
        }
        return returnResult;
    }

    public boolean addMessageLast(DataLoaderMessage message) {
        Jedis jedis = null;
        try {
            jedis = initJedis();
            jedis.rpush(ip, message.toString());
            return true;
        } catch (Exception e) {
            LOG.error("addMessageLast catch exception", e);
            if (jedis != null) {
                RedisResourceManager.getInstance().returnBrokenResource(jedis);
                jedis = null;
            }
        } finally {
            if (jedis != null) {
                RedisResourceManager.getInstance().returnResource(jedis);
            }
        }
        return false;
    }

    public String getAllMessage() {
        Jedis jedis = null;
        try {
            jedis = initJedis();
            return (jedis.lrange(ip, 0, 1)).toString();
        } catch (Exception e) {
            LOG.error("getAllMessage catch exception", e);
            if (jedis != null) {
                RedisResourceManager.getInstance().returnBrokenResource(jedis);
                jedis = null;
            }
            return "";
        } finally {
            if (jedis != null) {
                RedisResourceManager.getInstance().returnResource(jedis);
            }
        }

    }

    private Jedis initJedis() {
        Jedis jedis = RedisResourceManager.getInstance().getCache();
        jedis.select(dbNumber);
        return jedis;
    }
}
