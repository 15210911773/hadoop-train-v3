package com.bigdata.hadoop.hdfs;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * 自定义上下文(缓存)
 */
public class ImoocContext {

    private Map<Object, Object> cacheMap = Maps.newHashMap();

    public Map<Object, Object> getCacheMap() {
        return cacheMap;
    }

    public void write(Object key, Object value) {
        cacheMap.put(key, value);
    }

    public Object read(Object key) {
        return cacheMap.get(key);
    }

}
