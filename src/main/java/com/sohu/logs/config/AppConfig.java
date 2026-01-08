package com.sohu.logs.config;

import org.apache.hadoop.conf.Configuration;

import java.util.Properties;

public class AppConfig {
    private static final Properties props = new Properties();
    
    static {
        props.setProperty("hbase.zookeeper.quorum", "localhost");
        props.setProperty("hbase.zookeeper.property.clientPort", "2181");
        props.setProperty("hbase.table.name", "search_logs");
        props.setProperty("hbase.column.family", "cf");
        props.setProperty("hbase.salt.buckets", "16");
        props.setProperty("redis.host", "localhost");
        props.setProperty("redis.port", "6379");
        props.setProperty("redis.timeout", "2000");
        props.setProperty("mongodb.connection.string", "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000");
        props.setProperty("spark.master", "local[*]");
        props.setProperty("spark.app.name", "Sohu搜索日志统计分析");
        props.setProperty("data.load.batch.size", "5000");
        props.setProperty("data.load.encoding", "UTF-8");
        props.setProperty("search.result.limit", "100");
        props.setProperty("search.cache.enabled", "true");
    }
    
    public static String get(String key) {
        return props.getProperty(key);
    }
    
    public static String get(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }
    
    public static int getInt(String key, int defaultValue) {
        try {
            return Integer.parseInt(props.getProperty(key));
        } catch (NumberFormatException | NullPointerException e) {
            return defaultValue;
        }
    }
    
    public static Configuration createHBaseConfiguration() {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", get("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort", get("hbase.zookeeper.property.clientPort"));
        return conf;
    }
}