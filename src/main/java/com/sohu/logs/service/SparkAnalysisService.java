package com.sohu.logs.service;

import com.sohu.logs.config.AppConfig;
import com.sohu.logs.util.TimeUtils;
import com.sohu.logs.util.ChartGenerator;
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
import java.io.File;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkAnalysisService {
    private static final String TABLE_NAME = "search_logs";
    private static final byte[] CF = Bytes.toBytes("cf");
    
    public void executeSparkAnalysis() {
        System.out.println("\n=== Spark统计分析 ===");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        System.out.println("请按提示输入参数（可直接按回车使用默认值）:");
        
        System.out.print("1. 起始时间 (默认: 00:00:00): ");
        String startTime = safeReadLine(reader);
        if (startTime.isEmpty()) {
            startTime = "00:00:00";
        }
        
        System.out.print("2. 结束时间 (默认: 23:59:59): ");
        String endTime = safeReadLine(reader);
        if (endTime.isEmpty()) {
            endTime = "23:59:59";
        }
        
        System.out.print("3. 输出目录 (默认: output): ");
        String outputDir = safeReadLine(reader);
        if (outputDir.isEmpty()) {
            outputDir = "output";
        }
        
        System.out.print("4. ZooKeeper地址 (默认: " + AppConfig.get("hbase.zookeeper.quorum") + "): ");
        String zkQuorum = safeReadLine(reader);
        if (zkQuorum.isEmpty()) {
            zkQuorum = AppConfig.get("hbase.zookeeper.quorum");
        }
        
        System.out.print("5. ZooKeeper端口 (默认: " + AppConfig.get("hbase.zookeeper.property.clientPort") + "): ");
        String zkPort = safeReadLine(reader);
        if (zkPort.isEmpty()) {
            zkPort = AppConfig.get("hbase.zookeeper.property.clientPort");
        }
        
        System.out.println("\n即将执行统计分析:");
        System.out.println("时间范围: " + startTime + " 至 " + endTime);
        System.out.println("输出目录: " + outputDir);
        System.out.println("ZooKeeper地址: " + zkQuorum + ":" + zkPort);
        
        System.out.print("\n是否开始执行？(y/n): ");
        String confirm = safeReadLine(reader);
        if (confirm.isEmpty()) {
            System.out.println("\n输入结束，返回主菜单。");
            return;
        }
        
        if (confirm.equalsIgnoreCase("y") || confirm.equalsIgnoreCase("yes")) {
            String taskId = TaskLogger.logTaskStart("SPARK_ANALYSIS", null,
                    String.format("startTime=%s,endTime=%s,outputDir=%s,zkQuorum=%s,zkPort=%s,generateCharts=true",
                            startTime, endTime, outputDir, zkQuorum, zkPort));
            performSparkAnalysis(taskId, startTime, endTime, outputDir, zkQuorum, zkPort, true);
        } else {
            System.out.println("已取消统计分析");
        }
    }
    
    public void executeSparkAnalysisCommandLine(String startTime, String endTime, String outputDir, 
                                                String zkQuorum, String zkPort, boolean generateCharts) {
        System.out.println("\n=== Spark统计分析（命令行模式）===");
        System.out.println("时间范围: " + startTime + " 至 " + endTime);
        System.out.println("输出目录: " + outputDir);
        System.out.println("ZooKeeper地址: " + zkQuorum + ":" + zkPort);
        System.out.println("生成图表: " + (generateCharts ? "是" : "否"));
        System.out.println("\n开始执行统计分析...");
        
        String taskId = TaskLogger.logTaskStart("SPARK_ANALYSIS", null,
                String.format("startTime=%s,endTime=%s,outputDir=%s,zkQuorum=%s,zkPort=%s,generateCharts=%b",
                        startTime, endTime, outputDir, zkQuorum, zkPort, generateCharts));
        performSparkAnalysis(taskId, startTime, endTime, outputDir, zkQuorum, zkPort, generateCharts);
    }
    
    private void performSparkAnalysis(String taskId, String startTime, String endTime, String outputDir, 
                                    String zkQuorum, String zkPort, boolean generateCharts) {
        
        System.setProperty("java.awt.headless", "true");
        System.out.println("\n开始执行Spark统计分析...");
        
        try {
            
            Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", zkQuorum);
            hbaseConf.set("hbase.zookeeper.property.clientPort", zkPort);
            
            
            SparkConf sparkConf = new SparkConf()
                .setAppName(AppConfig.get("spark.app.name"))
                .setMaster(AppConfig.get("spark.master"))
                .set("spark.driver.host", "localhost")
                .set("spark.driver.bindAddress", "127.0.0.1")
                .set("spark.sql.legacy.timeParserPolicy", "LEGACY");
            
            try (JavaSparkContext sc = new JavaSparkContext(sparkConf);
                 SparkSession spark = SparkSession.builder().sparkContext(sc.sc()).getOrCreate();
                 Connection conn = ConnectionFactory.createConnection(hbaseConf)) {
                
                System.out.println("Spark上下文已创建，开始读取HBase数据...");


                List<org.apache.spark.sql.Row> rows = readDataFromHBase(conn, startTime, endTime);

                if (rows.isEmpty()) {
                    System.out.println("未找到符合条件的记录");
                    return;
                }

                Dataset<Row> searchLogsDF = spark.createDataFrame(rows,
                    org.apache.spark.sql.types.DataTypes.createStructType(Arrays.asList(
                        org.apache.spark.sql.types.DataTypes.createStructField("ts", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("user_id", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("query", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("rank", org.apache.spark.sql.types.DataTypes.IntegerType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("click_order", org.apache.spark.sql.types.DataTypes.IntegerType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("url", org.apache.spark.sql.types.DataTypes.StringType, true),
                        org.apache.spark.sql.types.DataTypes.createStructField("domain", org.apache.spark.sql.types.DataTypes.StringType, true)
                    )));

                searchLogsDF.createOrReplaceTempView("search_logs");

                performAnalysis(spark, startTime, endTime);
                
                saveResults(spark, startTime, endTime, outputDir, generateCharts);
                TaskLogger.logTaskSuccess(taskId, "SPARK_ANALYSIS", 
                        String.format("成功分析时间范围 %s 至 %s，结果保存在 %s", startTime, endTime, outputDir));
                
            } catch (Exception e) {
                System.err.println("Spark分析失败: " + e.getMessage());
                TaskLogger.logTaskFailure(taskId, "SPARK_ANALYSIS", e.getMessage());
                e.printStackTrace();
                throw e;
            }
            
        } catch (Exception e) {
            System.err.println("执行统计分析时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private List<org.apache.spark.sql.Row> readDataFromHBase(Connection conn, String startTime, String endTime) throws Exception {
        List<org.apache.spark.sql.Row> rows = new ArrayList<>();
        try (Table table = conn.getTable(TableName.valueOf(TABLE_NAME))) {
            
            Scan scan = new Scan();
            scan.addFamily(CF);
            scan.setCaching(100);
            
            
            setupTimeRangeScanForSpark(scan, startTime, endTime);
            
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
                    
                    
                    if (isTimeInRange(accessTime, startTime, endTime)) {
                        rows.add(RowFactory.create(accessTime, userId, query, rank, clickOrder, url, domain));
                        count++;
                        if (count % 1000 == 0) {
                            System.out.println("已读取 " + count + " 条记录...");
                        }
                    }
                }
                System.out.println("从HBase读取到 " + count + " 条记录");
            }
        }
        return rows;
    }
    
    private void performAnalysis(SparkSession spark, String startTime, String endTime) {
        System.out.println("\n=== 时段流量统计 (" + startTime + " 至 " + endTime + ") ===");
        
        
        Dataset<Row> totalSearches = spark.sql(
            "SELECT COUNT(*) as total_searches FROM search_logs"
        );
        System.out.println("总搜索次数:");
        totalSearches.show();
        
        
        Dataset<Row> queryStats = spark.sql(
            "SELECT query, COUNT(*) as search_count FROM search_logs " +
            "GROUP BY query ORDER BY search_count DESC LIMIT 8"
        );
        System.out.println("热门查询词统计（前8）:");
        queryStats.show();
        
        
        Dataset<Row> domainStats = spark.sql(
            "SELECT domain, COUNT(*) as visit_count FROM search_logs " +
            "GROUP BY domain ORDER BY visit_count DESC LIMIT 8"
        );
        System.out.println("网站访问量统计（前8）:");
        domainStats.show();
        
        
        Dataset<Row> userFrequency = spark.sql(
            "SELECT user_id, COUNT(*) as search_count FROM search_logs " +
            "GROUP BY user_id ORDER BY search_count DESC LIMIT 8"
        );
        System.out.println("用户搜索次数排名（前8）:");
        userFrequency.show();
        
        
        Dataset<Row> rankStats = spark.sql(
            "SELECT rank, COUNT(*) as visit_count FROM search_logs " +
            "GROUP BY rank ORDER BY rank"
        );
        System.out.println("不同排名结果被访问情况:");
        rankStats.show();
        
        
        Dataset<Row> top10RankStats = spark.sql(
            "SELECT rank, COUNT(*) as visit_count FROM search_logs " +
            "WHERE rank <= 10 GROUP BY rank ORDER BY rank"
        );
        System.out.println("排名1-10的结果被访问情况:");
        top10RankStats.show();
        
        
        Dataset<Row> clickOrderStats = spark.sql(
            "SELECT click_order, COUNT(*) as count FROM search_logs " +
            "GROUP BY click_order ORDER BY click_order"
        );
        System.out.println("点击顺序统计:");
        clickOrderStats.show();
    }
    
    private void saveResults(SparkSession spark, String startTime,
                           String endTime, String outputDir, boolean generateCharts) {
        System.out.println("\n=== 保存统计结果到文件 ===");
        
        String timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String baseOutputDir = outputDir + "/" + timestamp;
        
        
        Dataset<Row> queryStats = spark.sql(
            "SELECT query, COUNT(*) as search_count FROM search_logs " +
            "GROUP BY query ORDER BY search_count DESC LIMIT 100"
        );
        
        Dataset<Row> domainStats = spark.sql(
            "SELECT domain, COUNT(*) as visit_count FROM search_logs " +
            "GROUP BY domain ORDER BY visit_count DESC LIMIT 100"
        );
        
        Dataset<Row> userFrequency = spark.sql(
            "SELECT user_id, COUNT(*) as search_count FROM search_logs " +
            "GROUP BY user_id ORDER BY search_count DESC LIMIT 100"
        );
        
        Dataset<Row> rankStats = spark.sql(
            "SELECT rank, COUNT(*) as visit_count FROM search_logs " +
            "GROUP BY rank ORDER BY rank"
        );
        
        Dataset<Row> clickOrderStats = spark.sql(
            "SELECT click_order, COUNT(*) as count FROM search_logs " +
            "GROUP BY click_order ORDER BY click_order"
        );
        
        
        Dataset<Row> domainDistribution = spark.sql(
            "SELECT domain, COUNT(*) as count, " +
            "ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM search_logs), 2) as percentage " +
            "FROM search_logs GROUP BY domain ORDER BY count DESC LIMIT 100"
        );
        
        
        Dataset<Row> summaryReport = spark.sql(
            "SELECT '" + startTime + "' as start_time, '" + endTime + "' as end_time, " +
            "(SELECT COUNT(*) FROM search_logs) as total_searches, " +
            "(SELECT COUNT(DISTINCT user_id) FROM search_logs) as unique_users, " +
            "(SELECT COUNT(DISTINCT query) FROM search_logs) as unique_queries, " +
            "(SELECT COUNT(DISTINCT domain) FROM search_logs) as unique_domains"
        );
        
        
        queryStats.coalesce(1).write().mode("overwrite").csv(baseOutputDir + "/query_statistics");
        domainStats.coalesce(1).write().mode("overwrite").csv(baseOutputDir + "/domain_statistics");
        userFrequency.coalesce(1).write().mode("overwrite").csv(baseOutputDir + "/user_frequency");
        rankStats.coalesce(1).write().mode("overwrite").csv(baseOutputDir + "/rank_statistics");
        clickOrderStats.coalesce(1).write().mode("overwrite").csv(baseOutputDir + "/click_order_statistics");
        domainDistribution.coalesce(1).write().mode("overwrite").csv(baseOutputDir + "/domain_distribution");
        summaryReport.coalesce(1).write().mode("overwrite").csv(baseOutputDir + "/summary_report");
        
        
        if (generateCharts) {
            System.out.println("\n=== 生成统计图表 ===");
            try {
                String chartsDir = baseOutputDir + "/charts";
                new File(chartsDir).mkdirs();

                System.out.println("数据集信息:");
                System.out.println("  - queryStats 行数: " + queryStats.count() + ", 列: " + Arrays.toString(queryStats.columns()));
                System.out.println("  - domainStats 行数: " + domainStats.count() + ", 列: " + Arrays.toString(domainStats.columns()));
                System.out.println("  - userFrequency 行数: " + userFrequency.count() + ", 列: " + Arrays.toString(userFrequency.columns()));
                System.out.println("  - rankStats 行数: " + rankStats.count() + ", 列: " + Arrays.toString(rankStats.columns()));
                System.out.println("  - clickOrderStats 行数: " + clickOrderStats.count() + ", 列: " + Arrays.toString(clickOrderStats.columns()));
                System.out.println("  - domainDistribution 行数: " + domainDistribution.count() + ", 列: " + Arrays.toString(domainDistribution.columns()));
                
                
                ChartGenerator.generateTop8HorizontalBarChartWithOther(queryStats, 
                    "热门查询词统计（前8+其他）", "query", "search_count",
                    chartsDir + "/query_statistics_top8_with_other.png", 1400, 900);
                
                
                ChartGenerator.generateTop8HorizontalBarChartWithOther(domainStats,
                    "网站访问量统计（前8+其他）", "domain", "visit_count",
                    chartsDir + "/domain_statistics_top8_with_other.png", 1400, 900);
                
                
                ChartGenerator.generateTop8BarChartWithOther(userFrequency,
                    "用户搜索次数排名（前8+其他）", "user_id", "search_count",
                    chartsDir + "/user_frequency_top8_with_other.png", 1200, 800);
                
                
                ChartGenerator.generateBarChart(rankStats.limit(8),
                    "不同排名结果被访问情况（前8）", "rank", "visit_count",
                    chartsDir + "/rank_statistics_top8.png", 1200, 800);
                
                
                ChartGenerator.generateBarChart(clickOrderStats.limit(8),
                    "点击顺序统计（前8）", "click_order", "count",
                    chartsDir + "/click_order_statistics_top8.png", 1200, 800);
                
                
                ChartGenerator.generateTop8PieChartWithOther(domainDistribution,
                    "域名分布分析（前8+其他）", "domain", "count",
                    chartsDir + "/domain_distribution_pie_top8.png", 1000, 800);
                
                System.out.println("图表已生成到目录: " + chartsDir);
                
            } catch (Exception e) {
                System.err.println("生成图表时出错: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("\n跳过图表生成（根据参数设置）");
        }
        
        
        System.out.println("\n=== 统计汇总 ===");
        summaryReport.show();
        
        System.out.println("\n统计结果已保存到目录: " + baseOutputDir);
        System.out.println("包含以下文件:");
        System.out.println("  - query_statistics/ : 查询词统计");
        System.out.println("  - domain_statistics/ : 网站访问量统计");
        System.out.println("  - user_frequency/ : 用户使用频率统计");
        System.out.println("  - rank_statistics/ : 访问行为统计");
        System.out.println("  - click_order_statistics/ : 点击顺序统计");
        System.out.println("  - domain_distribution/ : 域名分布分析");
        System.out.println("  - summary_report/ : 汇总报告");
        if (generateCharts) {
            System.out.println("  - charts/ : 统计图表（PNG格式）");
        }
    }

    private void setupTimeRangeScanForSpark(Scan scan, String startTime, String endTime) {
        try {
            LocalDateTime startDateTime = TimeUtils.parseTimeOnlyToDateTime(startTime);
            LocalDateTime endDateTime = TimeUtils.parseTimeOnlyToDateTime(endTime);

            long startMillis = startDateTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
            long endMillis = endDateTime.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();


            long reverseStart = Long.MAX_VALUE - endMillis;
            long reverseEnd = Long.MAX_VALUE - startMillis;

            String reverseStartStr = String.format("%016d", reverseStart);
            String reverseEndStr = String.format("%016d", reverseEnd);


            scan.setStartRow(Bytes.toBytes("00|" + reverseStartStr));
            scan.setStopRow(Bytes.toBytes("16|" + reverseEndStr));

            System.out.println("设置时间范围扫描: startRow=00|" + reverseStartStr + ", stopRow=16|" + reverseEndStr);
        } catch (Exception e) {
            System.err.println("设置时间范围扫描失败: " + e.getMessage());
        }
    }
    
    private boolean isTimeInRange(String timeStr, String startTime, String endTime) {
        try {
            java.time.LocalTime time = java.time.LocalTime.parse(timeStr.trim(), 
                java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            java.time.LocalTime start = java.time.LocalTime.parse(startTime.trim(), 
                java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            java.time.LocalTime end = java.time.LocalTime.parse(endTime.trim(), 
                java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"));
            return !time.isBefore(start) && !time.isAfter(end);
        } catch (Exception e) {
            return false;
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