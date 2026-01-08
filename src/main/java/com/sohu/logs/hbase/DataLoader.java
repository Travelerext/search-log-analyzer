package com.sohu.logs.hbase;

import com.sohu.logs.model.LogRecord;
import com.sohu.logs.util.DomainUtils;
import com.sohu.logs.util.TaskLogger;
import com.sohu.logs.util.TimeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class DataLoader {
    private static final Logger log = LoggerFactory.getLogger(DataLoader.class);
    private static final String TABLE_NAME = "search_logs";
    
    public static void loadData(String filePath, String zkQuorum, String zkPort, 
                               int batchSize, String encoding) throws Exception {
        
        String taskId = TaskLogger.logTaskStart("DATA_LOAD", null, 
                String.format("file=%s,zk=%s:%s,batch=%d,encoding=%s", 
                        filePath, zkQuorum, zkPort, batchSize, encoding));
        
        try {
            
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zkQuorum);
            conf.set("hbase.zookeeper.property.clientPort", zkPort);
            
            
            try (Connection conn = ConnectionFactory.createConnection(conf)) {
                
                HBaseSchemaCreator.createTableIfNotExists(conn, TABLE_NAME);
                
                
                int totalRecords = processFile(conn, filePath, batchSize, encoding);
                
                
                TaskLogger.logTaskSuccess(taskId, "DATA_LOAD", 
                        String.format("成功加载 %d 条记录", totalRecords));
                
                log.info("数据加载完成，共 {} 条记录", totalRecords);
            }
        } catch (Exception e) {
            
            TaskLogger.logTaskFailure(taskId, "DATA_LOAD", e.getMessage());
            throw e;
        }
    }
    
    private static int processFile(Connection conn, String filePath, 
                                   int batchSize, String encoding) throws IOException {
        int totalRecords = 0;
        List<LogRecord> batch = new ArrayList<>(batchSize);
        
        try (HBaseWriter writer = new HBaseWriter(conn);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(Files.newInputStream(Paths.get(filePath)), encoding))) {
            
            String line;
            int lineNum = 0;
            
            while ((line = reader.readLine()) != null) {
                lineNum++;
                line = line.trim();
                
                
                if (line.isEmpty()) {
                    continue;
                }
                
                try {
                    LogRecord record = parseLogLine(line, lineNum);
                    if (record != null) {
                        batch.add(record);
                        
                        
                        if (batch.size() >= batchSize) {
                            writer.writeBatch(batch);
                            totalRecords += batch.size();
                            log.debug("已处理 {} 条记录", totalRecords);
                            batch.clear();
                        }
                    }
                } catch (Exception e) {
                    log.warn("解析第 {} 行失败: {}, 行内容: {}", lineNum, e.getMessage(), line);
                }
            }
            
            
            if (!batch.isEmpty()) {
                writer.writeBatch(batch);
                totalRecords += batch.size();
            }
            
            log.info("文件处理完成，共 {} 行，成功解析 {} 条记录", lineNum, totalRecords);
        }
        
        return totalRecords;
    }
    
    public static LogRecord parseLogLine(String line, int lineNum) {
        
        String[] parts = line.split("\t");
        
        if (parts.length != 5) {
            log.warn("第 {} 行格式错误，应有5个字段（制表符分割），实际 {} 个: {}", lineNum, parts.length, line);
            return null;
        }
        
        try {
            String accessTime = parts[0].trim();          
            String userId = parts[1].trim();             
            String query = parts[2].trim();              
            String rankClickStr = parts[3].trim();       
            String url = parts[4].trim();                
            
            
            if (!validateRequiredFields(accessTime, userId, url, lineNum, line)) {
                return null;
            }
            
            
            int[] rankAndClick = parseRankAndClick(rankClickStr, lineNum);
            if (rankAndClick == null) {
                return null;
            }
            int rank = rankAndClick[0];
            int clickOrder = rankAndClick[1];
            
            
            String domain = DomainUtils.extractDomain(url);
            if (domain.isEmpty()) {
                log.debug("第 {} 行无法提取域名: {}", lineNum, url);
            }
            
            
            long timestamp = TimeUtils.parseTimeOnly(accessTime);
            
            
            return createLogRecord(timestamp, accessTime, userId, query, rank, clickOrder, url, domain);
            
        } catch (NumberFormatException e) {
            log.warn("第 {} 行数值解析失败: {}, 错误: {}", lineNum, line, e.getMessage());
            return null;
        } catch (Exception e) {
            log.warn("第 {} 行解析失败: {}, 错误: {}", lineNum, line, e.getMessage());
            return null;
        }
    }
    
    private static boolean validateRequiredFields(String accessTime, String userId, String url, 
                                                 int lineNum, String line) {
        if (accessTime.isEmpty() || userId.isEmpty() || url.isEmpty()) {
            log.warn("第 {} 行必填字段为空: {}", lineNum, line);
            return false;
        }
        return true;
    }
    
    private static int[] parseRankAndClick(String rankClickStr, int lineNum) {
        String[] rankClickParts = rankClickStr.split("\\s+");
        if (rankClickParts.length != 2) {
            log.warn("第 {} 行排名和点击顺序格式错误: {}", lineNum, rankClickStr);
            return null;
        }
        
        try {
            int rank = Integer.parseInt(rankClickParts[0].trim());
            int clickOrder = Integer.parseInt(rankClickParts[1].trim());
            return new int[]{rank, clickOrder};
        } catch (NumberFormatException e) {
            log.warn("第 {} 行排名和点击顺序数值解析失败: {}, 错误: {}", lineNum, rankClickStr, e.getMessage());
            return null;
        }
    }
    
    private static LogRecord createLogRecord(long timestamp, String accessTime, String userId, 
                                           String query, int rank, int clickOrder, 
                                           String url, String domain) {
        LogRecord record = new LogRecord();
        record.ts = timestamp;
        record.tsStr = accessTime;
        record.userId = userId;
        record.query = query;
        record.rank = rank;
        record.clickOrder = clickOrder;
        record.url = url;
        record.domain = domain;
        record.tokens = null; 
        return record;
    }
    
}