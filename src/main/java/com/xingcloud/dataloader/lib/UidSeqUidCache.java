package com.xingcloud.dataloader.lib;


import com.xingcloud.util.Constants;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Uid和Seq的缓存类
 * User: IvyTang
 * Date: 12-10-11
 * Time: 下午3:55
 */
@Deprecated
public class UidSeqUidCache extends LinkedHashMap<String,Long> {

    public static final Log LOG = LogFactory.getLog(UidSeqUidCache.class);

    private static final float DEFAULT_LOAD_FACTOR = 0.7f;
    private int cacheCapacity = Constants.CACHECAPACITY;
    private final Lock lock = new ReentrantLock();


    public UidSeqUidCache(int cacheCapacity) {
        super(cacheCapacity, DEFAULT_LOAD_FACTOR, true);
        this.cacheCapacity = cacheCapacity;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry entry) {
        return size() > cacheCapacity;
    }


    public Long get(String key) {
        try {
            lock.lock();
            return (Long) super.get(key);
        } finally {
            lock.unlock();
        }

    }


    public Long put(String key, Long value) {
        try {
            lock.lock();
            return (Long) super.put(key, value);
        } finally {
            lock.unlock();
        }
    }


}