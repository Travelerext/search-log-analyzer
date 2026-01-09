package com.sohu.logs.cli;

import com.sohu.logs.service.*;
import com.sohu.logs.util.TaskLogger;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MenuHandler {
    
    private final SearchService searchService;
    private final DataLoadService dataLoadService;
    private final SparkAnalysisService sparkAnalysisService;
    private final SparkSearchService sparkSearchService;
    
    public MenuHandler() {
        this.searchService = new SearchService();
        this.dataLoadService = new DataLoadService();
        this.sparkAnalysisService = new SparkAnalysisService();
        this.sparkSearchService = new SparkSearchService();
    }
    
    public void showMainMenu() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        
        System.out.println("==========================================");
        System.out.println("      搜索日志查询与统计分析系统");
        System.out.println("==========================================");
        
        while (true) {
            System.out.println("\n请选择要执行的功能（输入数字）:");
            System.out.println("1. HBase交互式搜索");
            System.out.println("2. HBase行键精确查询");
            System.out.println("3. Spark统计分析");
            System.out.println("4. 数据加载到HBase");
            System.out.println("5. 数据加载到MongoDB");
            System.out.println("6. 清空数据表");
            System.out.println("7. 重试未完成的任务");
            System.out.println("8. Spark条件搜索");
            System.out.println("0. 退出程序");
            System.out.print("\n请选择 (0-8): ");
            
            String choice = safeReadLine(reader);
            
            if (choice.isEmpty()) {
                System.err.println("\n标准输入不可用，无法使用交互式菜单。");
                System.err.println("请使用命令行参数运行程序，例如:");
                System.err.println("  ./gradlew run --args=\"help\"  - 显示帮助");
                System.err.println("  ./gradlew run --args=\"rowkey \\\"0_user123#sohu\\\"\"  - 行键查询");
                System.err.println("  ./gradlew run --args=\"stats log.txt\"  - Spark分析");
                return;
            }
            
            switch (choice) {
                case "1":
                    searchService.executeInteractiveSearch();
                    break;
                case "2":
                    searchService.executeRowKeySearch();
                    break;
                case "3":
                    sparkAnalysisService.executeSparkAnalysis();
                    break;
                case "4":
                    dataLoadService.executeHBaseDataLoad();
                    break;
                case "5":
                    dataLoadService.executeMongoDataLoad();
                    break;
                case "6":
                    dataLoadService.executeCleanTable();
                    break;
                case "7":
                    retryIncompleteTasks();
                    break;
                  case "8":
                      sparkSearchService.executeInteractiveSearch();
                      break;
                  case "0":
                     System.out.println("感谢使用，再见！");
                     cleanup();
                     return;
                 default:
                     System.out.println("无效选择，请输入0-8之间的数字");
                    continue;
            }
            
            System.out.println("\n按回车键继续...");
            safeReadLine(reader);
        }
    }

    private void cleanup() {
        searchService.cleanup();
        dataLoadService.cleanup();
    }
    
    public void retryIncompleteTasks() {
        System.out.println("\n=== 重试未完成的任务 ===");
        List<String> incompleteTasks = TaskLogger.getIncompleteTasks();
        if (incompleteTasks.isEmpty()) {
            System.out.println("没有未完成的任务。");
            return;
        }
        
        System.out.println("找到 " + incompleteTasks.size() + " 个未完成的任务:");
        for (int i = 0; i < incompleteTasks.size(); i++) {
            String taskId = incompleteTasks.get(i);
            TaskLogger.TaskInfo taskInfo = TaskLogger.getTaskInfo(taskId);
            if (taskInfo != null) {
                System.out.println((i + 1) + ". " + taskId + " - " + taskInfo.getTaskType() + " - " + taskInfo.getParameters());
            } else {
                System.out.println((i + 1) + ". " + taskId + " - 无法获取任务信息");
            }
        }
        
        System.out.print("\n请输入要重试的任务编号 (输入0取消): ");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String input = safeReadLine(reader);
        if (input.isEmpty()) {
            System.out.println("输入无效，取消重试。");
            return;
        }
        try {
            int choice = Integer.parseInt(input);
            if (choice == 0) {
                System.out.println("取消重试。");
                return;
            }
            if (choice < 1 || choice > incompleteTasks.size()) {
                System.out.println("编号无效。");
                return;
            }
            String taskId = incompleteTasks.get(choice - 1);
            TaskLogger.TaskInfo taskInfo = TaskLogger.getTaskInfo(taskId);
            if (taskInfo == null) {
                System.out.println("无法获取任务信息，无法重试。");
                return;
            }
            System.out.println("正在重试任务: " + taskId);
            retryTask(taskInfo.getTaskType(), taskInfo.getParameters());
        } catch (NumberFormatException e) {
            System.out.println("请输入有效的数字。");
        }
    }
    
    private void retryTask(String taskType, String parameters) {
        System.out.println("重试任务类型: " + taskType + "，参数: " + parameters);
        try {
            Map<String, String> params = parseParameters(parameters);
            switch (taskType) {
                case "DATA_LOAD":
                    retryDataLoad(params);
                    break;
                case "MONGO_DATA_LOAD":
                    retryMongoDataLoad(params);
                    break;
                case "SPARK_ANALYSIS":
                    retrySparkAnalysis(params);
                    break;
                case "CLEAN_TABLE":
                    retryCleanTable();
                    break;
                default:
                    System.out.println("未知的任务类型: " + taskType + "，无法自动重试。");
                    break;
            }
        } catch (Exception e) {
            System.err.println("重试任务时出错: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private Map<String, String> parseParameters(String parameters) {
        Map<String, String> map = new HashMap<>();
        if (parameters == null || parameters.trim().isEmpty()) {
            return map;
        }
        String[] pairs = parameters.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split("=", 2);
            if (kv.length == 2) {
                map.put(kv[0].trim(), kv[1].trim());
            }
        }
        return map;
    }
    
    private void retryDataLoad(Map<String, String> params) {
        System.out.println("重试HBase数据加载...");
        String filePath = params.get("file");
        String zk = params.get("zk");
        if (zk == null) {
            zk = "localhost:2181";
        }
        String[] zkParts = zk.split(":");
        String zkQuorum = zkParts[0];
        String zkPort = zkParts.length > 1 ? zkParts[1] : "2181";
        String batchStr = params.get("batch");
        int batchSize = batchStr != null ? Integer.parseInt(batchStr) : 5000;
        String encoding = params.get("encoding");
        if (encoding == null) encoding = "UTF-8";
        
        try {
            dataLoadService.executeHBaseDataLoad(filePath, zkQuorum, zkPort, batchSize, encoding);
        } catch (Exception e) {
            System.err.println("重试数据加载失败: " + e.getMessage());
            throw new RuntimeException("重试失败", e);
        }
    }
    
    private void retryMongoDataLoad(Map<String, String> params) {
        System.out.println("重试MongoDB数据加载...");
        String filePath = params.get("file");
        String connectionString = params.get("connection");
        if (connectionString == null) {
            connectionString = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.5.10";
        }
        String batchStr = params.get("batch");
        int batchSize = batchStr != null ? Integer.parseInt(batchStr) : 5000;
        String encoding = params.get("encoding");
        if (encoding == null) encoding = "UTF-8";
        
        try {
            dataLoadService.executeMongoDataLoad(filePath, connectionString, batchSize, encoding);
        } catch (Exception e) {
            System.err.println("重试MongoDB数据加载失败: " + e.getMessage());
            throw new RuntimeException("重试失败", e);
        }
    }
    
    private void retrySparkAnalysis(Map<String, String> params) {
        System.out.println("重试Spark统计分析...");
        String startTime = params.get("startTime");
        if (startTime == null) startTime = "00:00:00";
        String endTime = params.get("endTime");
        if (endTime == null) endTime = "23:59:59";
        String outputDir = params.get("outputDir");
        if (outputDir == null) outputDir = "output";
        String zkQuorum = params.get("zkQuorum");
        if (zkQuorum == null) zkQuorum = "localhost";
        String zkPort = params.get("zkPort");
        if (zkPort == null) zkPort = "2181";
        String generateChartsStr = params.get("generateCharts");
        boolean generateCharts = generateChartsStr == null || Boolean.parseBoolean(generateChartsStr);
        
        try {
            sparkAnalysisService.executeSparkAnalysisCommandLine(startTime, endTime, outputDir, zkQuorum, zkPort, generateCharts);
        } catch (Exception e) {
            System.err.println("重试Spark分析失败: " + e.getMessage());
            throw new RuntimeException("重试失败", e);
        }
    }
    
    private void retryCleanTable() {
        System.out.println("重试清空数据表...");
        
        try {
            dataLoadService.executeCleanTableCommandLine();
        } catch (Exception e) {
            System.err.println("重试清空数据表失败: " + e.getMessage());
            throw new RuntimeException("重试失败", e);
        }
    }
    
    private static String safeReadLine(BufferedReader reader) {
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
    
    public static void printUsage() {
        System.out.println("搜索日志查询与统计分析系统");
        System.out.println("用法:");
        System.out.println("  无参数                 - 显示交互式主菜单");
        System.out.println("  help                  - 显示此帮助信息");
        System.out.println("  search                - 进入HBase交互式搜索模式");
        System.out.println("  rowkey <行键>         - 按行键查询HBase记录");
        System.out.println("  stats [起始时间] [结束时间] [输出目录] [ZooKeeper地址] [ZooKeeper端口] [是否生成图表]");
        System.out.println("                        - 执行Spark统计分析（从HBase）");
        System.out.println("                        - 是否生成图表: true/false (默认true)");
        System.out.println("  load <数据文件> [ZooKeeper地址] [ZooKeeper端口] [批量大小] [编码]");
        System.out.println("                        - 加载数据到HBase");
        System.out.println("  mongoload <数据文件> [MongoDB连接字符串] [批量大小] [编码]");
        System.out.println("                        - 加载数据到MongoDB");
        System.out.println("  clean [truncate]      - 清空数据表所有数据");
        System.out.println("  sparksearch <搜索条件> [ZooKeeper地址] [ZooKeeper端口] [是否显示详情]");
        System.out.println("                        - 执行Spark条件搜索（从HBase）");
        System.out.println("                        - 是否显示详情: true/false (默认true)");
        System.out.println();
        System.out.println("示例:");
        System.out.println("  ./search-log-analyzer rowkey \"0_user123#example.com\"");
        System.out.println("  ./search-log-analyzer stats 00:00:00 23:59:59 output localhost 2181 true");
        System.out.println("  ./search-log-analyzer stats 00:00:00 23:59:59 output localhost 2181 false  # 不生成图表");
        System.out.println("  ./search-log-analyzer load log.tsv localhost 2181");
        System.out.println("  ./search-log-analyzer sparksearch \"time:00:00:00|01:00:00 + user:user1\" localhost 2181 true");
    }
}