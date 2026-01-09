package com.sohu.logs.service;

import com.sohu.logs.config.AppConfig;
import com.sohu.logs.search.SearchCondition;
import com.sohu.logs.util.TaskLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class SparkSearchService {
    private static final String TABLE_NAME = "search_logs";
    private static final byte[] CF = Bytes.toBytes("cf");
    
    public void executeInteractiveSearch() {
        System.out.println("\n=== Spark交互式条件搜索 ===");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        System.out.println("请输入搜索条件，格式参考使用说明。");
        System.out.println("示例: time:00:00:00|01:00:00 + user:user1|user2 + query:旅游|美食");
        System.out.println("       domain:sohu|baidu + url:example.com|news.sohu.com + rank:1-10 + click:1-3");
        System.out.println("使用 '+' 组合多个条件，输入 'exit' 退出");
        
        while (true) {
            System.out.print("\nSpark搜索> ");
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
                System.err.println("  url:example.com|news.sohu.com");
                System.err.println("  rank:1-10");
                System.err.println("  click:1-3");
                System.err.println("使用 '+' 组合多个条件: time:... + user:...");
                continue;
            }
            
            System.out.print("请输入ZooKeeper地址 (默认: " + AppConfig.get("hbase.zookeeper.quorum") + "): ");
            String zkQuorum = safeReadLine(reader);
            if (zkQuorum.isEmpty()) {
                zkQuorum = AppConfig.get("hbase.zookeeper.quorum");
            }
            
            System.out.print("请输入ZooKeeper端口 (默认: " + AppConfig.get("hbase.zookeeper.property.clientPort") + "): ");
            String zkPort = safeReadLine(reader);
            if (zkPort.isEmpty()) {
                zkPort = AppConfig.get("hbase.zookeeper.property.clientPort");
            }
            
            System.out.print("是否显示详细结果? (y/n, 默认: y): ");
            String showDetails = safeReadLine(reader);
            boolean showDetailedResults = showDetails.isEmpty() || showDetails.equalsIgnoreCase("y");
            
            System.out.println("\n开始执行Spark搜索...");
            try {
                performSparkSearch(condition, zkQuorum, zkPort, showDetailedResults);
            } catch (Exception e) {
                System.err.println("Spark搜索执行失败: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    public void executeSparkSearchCommandLine(SearchCondition condition, String zkQuorum, String zkPort, boolean showDetailedResults) {
        System.out.println("\n=== Spark条件搜索（命令行模式）===");
        System.out.println("搜索条件: " + condition);
        System.out.println("ZooKeeper地址: " + zkQuorum + ":" + zkPort);
        System.out.println("显示详细结果: " + (showDetailedResults ? "是" : "否"));
        System.out.println("\n开始执行Spark搜索...");
        
        String taskId = TaskLogger.logTaskStart("SPARK_SEARCH", null,
                String.format("condition=%s,zkQuorum=%s,zkPort=%s,showDetails=%b",
                        condition.toCacheKey(), zkQuorum, zkPort, showDetailedResults));
        try {
            performSparkSearch(condition, zkQuorum, zkPort, showDetailedResults);
            TaskLogger.logTaskSuccess(taskId, "SPARK_SEARCH", 
                    String.format("成功执行Spark搜索，条件: %s", condition.toCacheKey()));
        } catch (Exception e) {
            System.err.println("Spark搜索执行失败: " + e.getMessage());
            TaskLogger.logTaskFailure(taskId, "SPARK_SEARCH", e.getMessage());
            throw new RuntimeException("Spark搜索失败", e);
        }
    }
    
    private void performSparkSearch(SearchCondition condition, String zkQuorum, String zkPort, boolean showDetailedResults) throws Exception {
        System.setProperty("java.awt.headless", "true");
        
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum);
        hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort);
        
        SparkConf sparkConf = new SparkConf()
            .setAppName("Spark-Search-" + AppConfig.get("spark.app.name"))
            .setMaster(AppConfig.get("spark.master"))
            .set("spark.driver.host", "localhost")
            .set("spark.driver.bindAddress", "127.0.0.1")
            .set("spark.sql.legacy.timeParserPolicy", "LEGACY");
        
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf);
             SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
             Connection conn = ConnectionFactory.createConnection(hbaseConf)) {
            
            System.out.println("Spark上下文已创建，开始读取HBase数据...");

            List<Row> rows = readDataFromHBase(conn, condition);

            if (rows.isEmpty()) {
                System.out.println("未找到符合条件的记录");
                return;
            }

            Dataset<Row> searchLogsDF = spark.createDataFrame(rows,
                DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("access_time", DataTypes.StringType, true),
                    DataTypes.createStructField("user_id", DataTypes.StringType, true),
                    DataTypes.createStructField("query", DataTypes.StringType, true),
                    DataTypes.createStructField("rank", DataTypes.IntegerType, true),
                    DataTypes.createStructField("click_order", DataTypes.IntegerType, true),
                    DataTypes.createStructField("url", DataTypes.StringType, true),
                    DataTypes.createStructField("domain", DataTypes.StringType, true)
                )));

            // Register as temporary view
            searchLogsDF.createOrReplaceTempView("search_logs");

            Dataset<Row> filteredDF = applySearchFilters(spark, condition);
            
            long count = filteredDF.count();
            System.out.printf("找到 %d 条记录\n", count);
            
            if (count > 0) {
                if (showDetailedResults) {
                    System.out.println("\n前10条记录:");
                    filteredDF.limit(10).show(false);
                } else {
                    System.out.println("\n记录统计:");
                    filteredDF.groupBy("user_id", "query", "domain")
                              .count()
                              .orderBy(org.apache.spark.sql.functions.col("count").desc())
                              .limit(10)
                              .show(false);
                }
                
                if (count > 100) {
                    System.out.println("警告: 搜索结果超过100条，建议添加更多过滤条件");
                }
            }
        }
    }
    
    private List<Row> readDataFromHBase(Connection conn, SearchCondition condition) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {
            
            Scan scan = new Scan();
            scan.addFamily(CF);
            scan.setCaching(100);
            
            if (condition.hasTimeRange()) {
                setupTimeRangeScan(scan, condition);
            }
            
            try (ResultScanner scanner = table.getScanner(scan)) {
                int count = 0;
                for (Result result : scanner) {
                    String accessTime = Bytes.toString(result.getValue(CF, Bytes.toBytes("ts")));
                    String userId = Bytes.toString(result.getValue(CF, Bytes.toBytes("user")));
                    String query = Bytes.toString(result.getValue(CF, Bytes.toBytes("query")));
                    int rank = Bytes.toInt(result.getValue(CF, Bytes.toBytes("rank")));
                    int clickOrder = Bytes.toInt(result.getValue(CF, Bytes.toBytes("click")));
                    String url = Bytes.toString(result.getValue(CF, Bytes.toBytes("url")));
                    String domain = Bytes.toString(result.getValue(CF, Bytes.toBytes("domain")));
                    
                    rows.add(RowFactory.create(accessTime, userId, query, rank, clickOrder, url, domain));
                    count++;
                    if (count % 1000 == 0) {
                        System.out.println("已读取 " + count + " 条记录...");
                    }
                }
                System.out.println("从HBase读取到 " + count + " 条记录");
            }
        }
        return rows;
    }
    
    private Dataset<Row> applySearchFilters(SparkSession spark, SearchCondition condition) {
        StringBuilder whereClause = new StringBuilder("1=1");
        
        if (condition.hasTimeRange()) {
            String startTimeStr = condition.getStartTime().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            String endTimeStr = condition.getEndTime().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
            whereClause.append(" AND access_time >= '").append(startTimeStr).append("'");
            whereClause.append(" AND access_time <= '").append(endTimeStr).append("'");
        }
        
        if (condition.hasUserIds()) {
            List<String> userIds = condition.getUserIds();
            whereClause.append(" AND (");
            for (int i = 0; i < userIds.size(); i++) {
                if (i > 0) whereClause.append(" OR ");
                whereClause.append("user_id = '").append(userIds.get(i)).append("'");
            }
            whereClause.append(")");
        }
        
        if (condition.hasQueryKeywords()) {
            List<String> keywords = condition.getQueryKeywords();
            whereClause.append(" AND (");
            for (int i = 0; i < keywords.size(); i++) {
                if (i > 0) whereClause.append(" OR ");
                whereClause.append("query LIKE '%").append(keywords.get(i)).append("%'");
            }
            whereClause.append(")");
        }
        
        if (condition.hasDomainKeywords()) {
            List<String> keywords = condition.getDomainKeywords();
            whereClause.append(" AND (");
            for (int i = 0; i < keywords.size(); i++) {
                if (i > 0) whereClause.append(" OR ");
                whereClause.append("domain LIKE '%").append(keywords.get(i)).append("%'");
            }
            whereClause.append(")");
        }

        if (condition.hasUrlKeywords()) {
            List<String> keywords = condition.getUrlKeywords();
            whereClause.append(" AND (");
            for (int i = 0; i < keywords.size(); i++) {
                if (i > 0) whereClause.append(" OR ");
                whereClause.append("url LIKE '%").append(keywords.get(i)).append("%'");
            }
            whereClause.append(")");
        }
        
        if (condition.hasRankRange()) {
            whereClause.append(" AND rank >= ").append(condition.getMinRank());
            whereClause.append(" AND rank <= ").append(condition.getMaxRank());
        }
        
        if (condition.hasClickOrderRange()) {
            whereClause.append(" AND click_order >= ").append(condition.getMinClickOrder());
            whereClause.append(" AND click_order <= ").append(condition.getMaxClickOrder());
        }
        
        String sql = "SELECT * FROM search_logs WHERE " + whereClause;
        System.out.println("执行Spark SQL: " + sql);
        
        return spark.sql(sql);
    }
    
    private void setupTimeRangeScan(Scan scan, SearchCondition condition) {
        try {
            String reverseStart = condition.getReverseStartTimeStr();
            String reverseEnd = condition.getReverseEndTimeStr();
            
            scan.setStartRow(Bytes.toBytes("00|" + reverseStart));
            scan.setStopRow(Bytes.toBytes("16|" + reverseEnd));
            
            System.out.println("设置时间范围扫描: startRow=00|" + reverseStart + ", stopRow=16|" + reverseEnd);
        } catch (Exception e) {
            System.err.println("设置时间范围扫描失败: " + e.getMessage());
        }
    }
    
    public List<Map<String, Object>> executeSparkSearchAsList(SearchCondition condition, String zkQuorum, String zkPort) throws Exception {
        System.setProperty("java.awt.headless", "true");
        
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", zkQuorum);
        hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort);
        
        SparkConf sparkConf = new SparkConf()
            .setAppName("Spark-Search-" + AppConfig.get("spark.app.name"))
            .setMaster(AppConfig.get("spark.master"))
            .set("spark.driver.host", "localhost")
            .set("spark.driver.bindAddress", "127.0.0.1")
            .set("spark.sql.legacy.timeParserPolicy", "LEGACY");
        
        try (JavaSparkContext sc = new JavaSparkContext(sparkConf);
             SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
             Connection conn = ConnectionFactory.createConnection(hbaseConf)) {
            
            List<Row> rows = readDataFromHBase(conn, condition);

            if (rows.isEmpty()) {
                return new java.util.ArrayList<>();
            }

            Dataset<Row> searchLogsDF = spark.createDataFrame(rows,
                DataTypes.createStructType(Arrays.asList(
                    DataTypes.createStructField("access_time", DataTypes.StringType, true),
                    DataTypes.createStructField("user_id", DataTypes.StringType, true),
                    DataTypes.createStructField("query", DataTypes.StringType, true),
                    DataTypes.createStructField("rank", DataTypes.IntegerType, true),
                    DataTypes.createStructField("click_order", DataTypes.IntegerType, true),
                    DataTypes.createStructField("url", DataTypes.StringType, true),
                    DataTypes.createStructField("domain", DataTypes.StringType, true)
                )));

            searchLogsDF.createOrReplaceTempView("search_logs");

            Dataset<Row> filteredDF = applySearchFilters(spark, condition);
            
            List<Map<String, Object>> results = new java.util.ArrayList<>();
            List<Row> resultRows = filteredDF.collectAsList();
            for (Row row : resultRows) {
                Map<String, Object> map = new java.util.HashMap<>();
                map.put("accessTime", row.getAs("access_time"));
                map.put("userId", row.getAs("user_id"));
                map.put("query", row.getAs("query"));
                map.put("rank", row.getAs("rank"));
                map.put("clickOrder", row.getAs("click_order"));
                map.put("url", row.getAs("url"));
                map.put("domain", row.getAs("domain"));
                results.add(map);
            }
            return results;
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
}