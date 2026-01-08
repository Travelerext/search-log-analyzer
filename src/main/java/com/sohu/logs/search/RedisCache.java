package com.sohu.logs.search;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.List;

public class RedisCache {
    private static final Logger log = LoggerFactory.getLogger(RedisCache.class);
    private static final int MAX_CACHE_SIZE = 20; 
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final int REDIS_TIMEOUT = 5000;
    
    private final JedisPool jedisPool;
    
    public RedisCache() {
        this(REDIS_HOST, REDIS_PORT);
    }
    
    public RedisCache(String host, int port) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        
        this.jedisPool = new JedisPool(poolConfig, host, port, REDIS_TIMEOUT);
        log.info("Redis连接池初始化完成，地址: {}:{}", host, port);
    }
    
    public List<String> get(String searchKey) {
        try (Jedis jedis = jedisPool.getResource()) {
            
            if (!jedis.exists(searchKey)) {
                return null;
            }
            
            
            String cachedValue = jedis.get(searchKey);
            if (cachedValue == null || cachedValue.isEmpty()) {
                return null;
            }
            
            
            String[] rowKeys = cachedValue.split("\\|\\|\\|");
            List<String> result = new ArrayList<>();
            for (String key : rowKeys) {
                if (!key.isEmpty()) {
                    result.add(key);
                }
            }
            
            
            long timestamp = System.currentTimeMillis();
            jedis.zadd("search_cache_access", timestamp, searchKey);
            
            log.debug("从Redis缓存获取搜索结果，条件: {}，结果数量: {}", searchKey, result.size());
            return result;
        } catch (JedisException e) {
            log.error("Redis操作失败", e);
            return null;
        }
    }
    
    public void put(String searchKey, List<String> rowKeys) {
        if (rowKeys == null || rowKeys.isEmpty()) {
            return;
        }
        
        try (Jedis jedis = jedisPool.getResource()) {
            
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < rowKeys.size(); i++) {
                if (i > 0) {
                    sb.append("|||");
                }
                sb.append(rowKeys.get(i));
            }
            
            
            jedis.set(searchKey, sb.toString());
            
            
            long timestamp = System.currentTimeMillis();
            jedis.zadd("search_cache_access", timestamp, searchKey);
            
            
            enforceCacheSize(jedis);
            
            log.debug("将搜索结果存入Redis缓存，条件: {}，结果数量: {}", searchKey, rowKeys.size());
        } catch (JedisException e) {
            log.error("Redis操作失败", e);
        }
    }
    
    private void enforceCacheSize(Jedis jedis) {
            
            long cacheSize = jedis.zcard("search_cache_access");
            
            if (cacheSize > MAX_CACHE_SIZE) {
                
                List<String> oldKeys = jedis.zrange("search_cache_access", 0, cacheSize - MAX_CACHE_SIZE - 1);
                for (String key : oldKeys) {
                    jedis.del(key);
                    jedis.zrem("search_cache_access", key);
                    log.debug("删除旧缓存: {}", key);
                }
            }
    }
    
    public long size() {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.zcard("search_cache_access");
        } catch (JedisException e) {
            log.error("获取Redis缓存数量失败", e);
            return 0;
        }
    }
    
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            log.info("Redis连接池已关闭");
        }
    }
}