package com.sohu.logs.mongo;

import com.sohu.logs.model.LogRecord;
import com.sohu.logs.hbase.DataLoader;
import com.sohu.logs.util.TaskLogger;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class MongoDBDataLoader {
    private static final Logger log = LoggerFactory.getLogger(MongoDBDataLoader.class);
    private static final String DEFAULT_DATABASE = "search_logs";
    private static final String DEFAULT_COLLECTION = "logs";
    
    public static void loadData(String filePath, String connectionString, 
                               int batchSize, String encoding) throws Exception {
        
        String taskId = TaskLogger.logTaskStart("MONGO_DATA_LOAD", null,
                String.format("file=%s,connection=%s,batch=%d,encoding=%s",
                        filePath, connectionString, batchSize, encoding));
        
        try (MongoClient mongoClient = MongoClients.create(connectionString)) {
            MongoDatabase database = mongoClient.getDatabase(DEFAULT_DATABASE);
            MongoCollection<Document> collection = database.getCollection(DEFAULT_COLLECTION);
            
            int totalRecords = processFile(collection, filePath, batchSize, encoding);
            
            TaskLogger.logTaskSuccess(taskId, "MONGO_DATA_LOAD",
                    String.format("成功加载 %d 条记录", totalRecords));
            
            log.info("数据加载完成，共 {} 条记录", totalRecords);
        } catch (Exception e) {
            TaskLogger.logTaskFailure(taskId, "MONGO_DATA_LOAD", e.getMessage());
            throw e;
        }
    }
    
    private static int processFile(MongoCollection<Document> collection, String filePath,
                                   int batchSize, String encoding) throws IOException {
        int totalRecords = 0;
        List<Document> batch = new ArrayList<>(batchSize);
        
        try (BufferedReader reader = new BufferedReader(
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
                    LogRecord record = DataLoader.parseLogLine(line, lineNum);
                    if (record != null) {
                        Document doc = convertToDocument(record);
                        batch.add(doc);
                        
                        if (batch.size() >= batchSize) {
                            collection.insertMany(batch);
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
                collection.insertMany(batch);
                totalRecords += batch.size();
            }
            
            log.info("文件处理完成，共 {} 行，成功解析 {} 条记录", lineNum, totalRecords);
        }
        
        return totalRecords;
    }
    

    
    private static Document convertToDocument(LogRecord record) {
        return new Document()
                .append("ts", record.ts)
                .append("tsStr", record.tsStr)
                .append("userId", record.userId)
                .append("query", record.query)
                .append("rank", record.rank)
                .append("clickOrder", record.clickOrder)
                .append("url", record.url)
                .append("domain", record.domain)
                .append("tokens", record.tokens);
    }
}