package com.sohu.logs.service;

import com.sohu.logs.config.AppConfig;
import com.sohu.logs.hbase.DataLoader;
import com.sohu.logs.hbase.HBaseCleaner;
import com.sohu.logs.mongo.MongoDBDataLoader;
import com.sohu.logs.search.RedisCache;
import com.sohu.logs.util.TaskLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class DataLoadService {
    private static final Logger log = LoggerFactory.getLogger(DataLoadService.class);
    private RedisCache redisCache;
    
    public DataLoadService() {
        this.redisCache = new RedisCache();
    }
    
    public void executeHBaseDataLoad() {
        System.out.println("\n=== 数据加载到HBase ===");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        System.out.println("请按提示输入参数（可直接按回车使用默认值）:");
        
        System.out.print("1. 数据文件路径 (默认: file:///data/log.tsv): ");
        String filePath = safeReadLine(reader);
        if (filePath.isEmpty()) {
            filePath = "file:///data/log.tsv";
        }
        
        System.out.print("2. ZooKeeper地址 (默认: " + AppConfig.get("hbase.zookeeper.quorum") + "): ");
        String zkQuorum = safeReadLine(reader);
        if (zkQuorum.isEmpty()) {
            zkQuorum = AppConfig.get("hbase.zookeeper.quorum");
        }
        
        System.out.print("3. ZooKeeper端口 (默认: " + AppConfig.get("hbase.zookeeper.property.clientPort") + "): ");
        String zkPort = safeReadLine(reader);
        if (zkPort.isEmpty()) {
            zkPort = AppConfig.get("hbase.zookeeper.property.clientPort");
        }
        
        System.out.print("4. 批量大小 (默认: " + AppConfig.get("data.load.batch.size") + "): ");
        String batchSizeStr = safeReadLine(reader);
        int batchSize = AppConfig.getInt("data.load.batch.size", 5000);
        if (!batchSizeStr.isEmpty()) {
            try {
                batchSize = Integer.parseInt(batchSizeStr);
            } catch (NumberFormatException e) {
                System.out.println("无效数字，使用默认值" + batchSize);
            }
        }
        
        System.out.print("5. 文件编码 (默认: " + AppConfig.get("data.load.encoding") + "): ");
        String encoding = safeReadLine(reader);
        if (encoding.isEmpty()) {
            encoding = AppConfig.get("data.load.encoding");
        }
        
        System.out.println("\n即将加载数据:");
        System.out.println("数据文件: " + filePath);
        System.out.println("ZooKeeper地址: " + zkQuorum + ":" + zkPort);
        System.out.println("批量大小: " + batchSize);
        System.out.println("文件编码: " + encoding);
        
        System.out.print("\n是否开始执行？(y/n): ");
        String confirm = safeReadLine(reader);
        if (confirm.isEmpty()) {
            System.out.println("\n输入结束，返回主菜单。");
            return;
        }
        
        if (confirm.equalsIgnoreCase("y") || confirm.equalsIgnoreCase("yes")) {
            try {
                DataLoader.loadData(filePath, zkQuorum, zkPort, batchSize, encoding);
            } catch (Exception e) {
                System.err.println("数据加载失败: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("已取消数据加载");
        }
    }
    
    public void executeHBaseDataLoad(String filePath, String zkQuorum, String zkPort, int batchSize, String encoding) {
        System.out.println("\n=== 数据加载到HBase（命令行模式）===");
        System.out.println("数据文件: " + filePath);
        System.out.println("ZooKeeper地址: " + zkQuorum + ":" + zkPort);
        System.out.println("批量大小: " + batchSize);
        System.out.println("文件编码: " + encoding);
        System.out.println("\n开始执行数据加载...");
        
        try {
            DataLoader.loadData(filePath, zkQuorum, zkPort, batchSize, encoding);
        } catch (Exception e) {
            System.err.println("数据加载失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("数据加载失败: " + e.getMessage(), e);
        }
    }
    
    public void executeMongoDataLoad() {
        System.out.println("\n=== 数据加载到MongoDB ===");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        System.out.println("请按提示输入参数（可直接按回车使用默认值）:");
        
        System.out.print("1. 数据文件路径 (默认: file:///data/log.tsv): ");
        String filePath = safeReadLine(reader);
        if (filePath.isEmpty()) {
            filePath = "file:///data/log.tsv";
        }
        
        System.out.print("2. MongoDB连接字符串 (默认: " + AppConfig.get("mongodb.connection.string") + "): ");
        String connectionString = safeReadLine(reader);
        if (connectionString.isEmpty()) {
            connectionString = AppConfig.get("mongodb.connection.string");
        }
        
        System.out.print("3. 批量大小 (默认: " + AppConfig.get("data.load.batch.size") + "): ");
        String batchSizeStr = safeReadLine(reader);
        int batchSize = AppConfig.getInt("data.load.batch.size", 5000);
        if (!batchSizeStr.isEmpty()) {
            try {
                batchSize = Integer.parseInt(batchSizeStr);
            } catch (NumberFormatException e) {
                System.out.println("无效数字，使用默认值" + batchSize);
            }
        }
        
        System.out.print("4. 文件编码 (默认: " + AppConfig.get("data.load.encoding") + "): ");
        String encoding = safeReadLine(reader);
        if (encoding.isEmpty()) {
            encoding = AppConfig.get("data.load.encoding");
        }
        
        System.out.println("\n即将加载数据:");
        System.out.println("数据文件: " + filePath);
        System.out.println("MongoDB连接字符串: " + connectionString);
        System.out.println("批量大小: " + batchSize);
        System.out.println("文件编码: " + encoding);
        
        System.out.print("\n是否开始执行？(y/n): ");
        String confirm = safeReadLine(reader);
        if (confirm.isEmpty()) {
            System.out.println("\n输入结束，返回主菜单。");
            return;
        }
        
        if (confirm.equalsIgnoreCase("y") || confirm.equalsIgnoreCase("yes")) {
            try {
                MongoDBDataLoader.loadData(filePath, connectionString, batchSize, encoding);
            } catch (Exception e) {
                System.err.println("数据加载失败: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("已取消数据加载");
        }
    }
    
    public void executeMongoDataLoad(String filePath, String connectionString, int batchSize, String encoding) {
        System.out.println("\n=== 数据加载到MongoDB（命令行模式）===");
        System.out.println("数据文件: " + filePath);
        System.out.println("MongoDB连接字符串: " + connectionString);
        System.out.println("批量大小: " + batchSize);
        System.out.println("文件编码: " + encoding);
        System.out.println("\n开始执行数据加载...");
        
        try {
            MongoDBDataLoader.loadData(filePath, connectionString, batchSize, encoding);
        } catch (Exception e) {
            System.err.println("数据加载失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("数据加载失败: " + e.getMessage(), e);
        }
    }
    
    public void executeCleanTable() {
        System.out.println("\n=== 清空数据表 ===");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        System.out.println("警告: 此操作将删除表中所有数据，不可恢复！");
        System.out.print("确认清空表吗？(输入 yes 确认): ");
        
        String confirm = safeReadLine(reader);
        if (confirm.isEmpty()) {
            System.out.println("\n输入结束，返回主菜单。");
            return;
        }
        
        if (!"yes".equalsIgnoreCase(confirm)) {
            System.out.println("操作已取消");
            return;
        }
        
        String taskId = TaskLogger.logTaskStart("CLEAN_TABLE", null, "");
        
        Configuration conf = AppConfig.createHBaseConfiguration();
        
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            HBaseCleaner.clearAllData(conn);
            TaskLogger.logTaskSuccess(taskId, "CLEAN_TABLE", "表已清空");
            System.out.println("表已清空");
        } catch (org.apache.hadoop.hbase.client.RetriesExhaustedException e) {
            String error = "无法连接到HBase，请检查HBase服务是否已启动。";
            TaskLogger.logTaskFailure(taskId, "CLEAN_TABLE", error + " " + e.getMessage());
            System.err.println(error);
            System.err.println("错误详情: " + e.getMessage());
        } catch (java.net.ConnectException e) {
            String error = "连接被拒绝，请确保ZooKeeper (" + AppConfig.get("hbase.zookeeper.quorum") + 
                         ":" + AppConfig.get("hbase.zookeeper.property.clientPort") + ") 和HBase正在运行。";
            TaskLogger.logTaskFailure(taskId, "CLEAN_TABLE", error + " " + e.getMessage());
            System.err.println(error);
            System.err.println("错误详情: " + e.getMessage());
        } catch (Exception e) {
            TaskLogger.logTaskFailure(taskId, "CLEAN_TABLE", e.getMessage());
            System.err.println("清除数据失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void executeCleanTableCommandLine() {
        Configuration conf = AppConfig.createHBaseConfiguration();
        
        String taskId = TaskLogger.logTaskStart("CLEAN_TABLE", null, "");
        
        try (Connection conn = ConnectionFactory.createConnection(conf)) {
            System.out.println("\n=== 清空数据表 ===");
            System.out.println("警告: 此操作将删除表中所有数据，不可恢复！");
            HBaseCleaner.clearAllData(conn);
            TaskLogger.logTaskSuccess(taskId, "CLEAN_TABLE", "表已清空");
            System.out.println("表已清空");
        } catch (org.apache.hadoop.hbase.client.RetriesExhaustedException e) {
            String error = "无法连接到HBase，请检查HBase服务是否已启动。";
            TaskLogger.logTaskFailure(taskId, "CLEAN_TABLE", error + " " + e.getMessage());
            System.err.println(error);
            System.err.println("错误详情: " + e.getMessage());
            throw new RuntimeException(error, e);
        } catch (java.net.ConnectException e) {
            String error = "连接被拒绝，请确保ZooKeeper (" + AppConfig.get("hbase.zookeeper.quorum") + 
                         ":" + AppConfig.get("hbase.zookeeper.property.clientPort") + ") 和HBase正在运行。";
            TaskLogger.logTaskFailure(taskId, "CLEAN_TABLE", error + " " + e.getMessage());
            System.err.println(error);
            System.err.println("错误详情: " + e.getMessage());
            throw new RuntimeException(error, e);
        } catch (Exception e) {
            TaskLogger.logTaskFailure(taskId, "CLEAN_TABLE", e.getMessage());
            System.err.println("清除数据失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("清除数据失败: " + e.getMessage(), e);
        }
    }
    
    private String safeReadLine(BufferedReader reader) {
        try {
            String line = reader.readLine();
            if (line == null) {
                return "";
            }
            return line.trim();
        } catch (Exception e) {
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