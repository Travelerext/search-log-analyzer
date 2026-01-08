package com.sohu.logs.service;

import com.sohu.logs.config.AppConfig;
import com.sohu.logs.search.SearchCondition;
import com.sohu.logs.search.SearchEngine;
import com.sohu.logs.search.RedisCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class SearchService {
    private static final Logger log = LoggerFactory.getLogger(SearchService.class);
    private RedisCache redisCache;
    
    public SearchService() {
        this.redisCache = new RedisCache();
    }
    
    public void executeInteractiveSearch() {
        System.out.println("\n=== HBase交互式搜索 ===");
        Configuration conf = AppConfig.createHBaseConfiguration();
        
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            interactiveSearch(conn);
        } catch (org.apache.hadoop.hbase.client.RetriesExhaustedException e) {
            System.err.println("无法连接到HBase，请检查HBase服务是否已启动。");
            System.err.println("错误详情: " + e.getMessage());
        } catch (java.net.ConnectException e) {
            System.err.println("连接被拒绝，请确保ZooKeeper (" + AppConfig.get("hbase.zookeeper.quorum") + 
                             ":" + AppConfig.get("hbase.zookeeper.property.clientPort") + ") 和HBase正在运行。");
            System.err.println("错误详情: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("HBase搜索执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void executeRowKeySearch() {
        System.out.println("\n=== HBase行键精确查询 ===");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        System.out.print("请输入要查询的行键: ");
        String rowKey = safeReadLine(reader);
        
        if (rowKey.isEmpty()) {
            System.out.println("行键不能为空");
            return;
        }
        
        executeRowKeySearch(rowKey);
    }
    
    public void executeRowKeySearch(String rowKey) {
        System.out.println("\n=== HBase行键精确查询 ===");
        System.out.println("查询行键: " + rowKey);
        
        Configuration conf = AppConfig.createHBaseConfiguration();
        
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            searchByRowKey(conn, rowKey);
        } catch (org.apache.hadoop.hbase.client.RetriesExhaustedException e) {
            System.err.println("无法连接到HBase，请检查HBase服务是否已启动。");
            System.err.println("错误详情: " + e.getMessage());
            System.exit(1);
        } catch (java.net.ConnectException e) {
            System.err.println("连接被拒绝，请确保ZooKeeper (" + AppConfig.get("hbase.zookeeper.quorum") + 
                             ":" + AppConfig.get("hbase.zookeeper.property.clientPort") + ") 和HBase正在运行。");
            System.err.println("错误详情: " + e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("行键查询失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private void interactiveSearch(Connection conn) {
        SearchEngine searchEngine = new SearchEngine(conn, redisCache);
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        System.out.println("交互式搜索模式 (输入 'exit' 退出)");
        System.out.println("请输入搜索条件，格式参考使用说明。");
        
        while (true) {
            System.out.print("\n搜索> ");
            String input = safeReadLine(reader);
            if (input.isEmpty()) {
                System.out.println("\n输入结束，退出搜索模式。");
                break;
            }
            if (input.equalsIgnoreCase("exit")) {
                break;
            }
            
            SearchCondition condition;
            try {
                condition = SearchCondition.parse(input);
                System.out.println("搜索条件: " + condition);
            } catch (Exception e) {
                System.err.println("搜索条件格式错误: " + e.getMessage());
                System.err.println("请参考以下格式示例:");
                System.err.println("  time:00:00:00|01:00:00");
                System.err.println("  user:user1|user2");
                System.err.println("  query:旅游|美食");
                System.err.println("  domain:sohu|baidu");
                System.err.println("  rank:1-10");
                System.err.println("  click:1-3");
                System.err.println("使用 '+' 组合多个条件: time:... + user:...");
                continue;
            }
            
            try {
                long startTime = System.currentTimeMillis();
                List<Result> results = searchEngine.search(condition);
                long endTime = System.currentTimeMillis();
                
                System.out.printf("找到 %d 条记录，耗时 %d 毫秒\n", results.size(), endTime - startTime);
                
                if (!results.isEmpty()) {
                    System.out.println("\n前10条记录:");
                    int limit = Math.min(10, results.size());
                    for (int i = 0; i < limit; i++) {
                        SearchEngine.printResult(results.get(i));
                    }
                    if (results.size() > 10) {
                        System.out.printf("... 还有 %d 条记录\n", results.size() - 10);
                    }
                }
            } catch (IOException e) {
                System.err.println("搜索执行失败，请检查HBase连接: " + e.getMessage());
            }
        }
    }
    
    private void searchByRowKey(Connection conn, String rowKey) throws IOException {
        SearchEngine searchEngine = new SearchEngine(conn);
        Result result = searchEngine.getByRowKey(rowKey);
        
        if (result.isEmpty()) {
            System.out.println("未找到行键对应的记录: " + rowKey);
        } else {
            SearchEngine.printResult(result);
        }
    }
    
    private String safeReadLine(BufferedReader reader) {
        try {
            String line = reader.readLine();
            if (line == null) {
                return "";
            }
            return line.trim();
        } catch (IOException e) {
            return "";
        }
    }
    
    public void cleanup() {
        if (redisCache != null) {
            try {
                redisCache.close();
                log.info("Redis连接池已关闭");
            } catch (Exception e) {
                log.error("关闭Redis连接池时出错", e);
            }
        }
    }
}