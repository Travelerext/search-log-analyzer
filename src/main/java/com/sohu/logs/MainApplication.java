package com.sohu.logs;

import com.sohu.logs.cli.CommandLineParser;
import com.sohu.logs.cli.MenuHandler;
import com.sohu.logs.service.*;

public class MainApplication {

    public static void main(String[] args) {
        try {
            CommandLineParser.Command command = CommandLineParser.parse(args);
            
            switch (command.getName()) {
                case "menu":
                    new MenuHandler().showMainMenu();
                    break;
                    
                case "rowkey":
                    handleRowKeyCommand(command);
                    break;
                    
                case "stats":
                    handleStatsCommand(command);
                    break;
                    
                case "search":
                    new SearchService().executeInteractiveSearch();
                    break;
                    
                case "sparksearch":
                    handleSparkSearchCommand(command);
                    break;
                    
                case "load":
                    handleLoadCommand(command);
                    break;
                    
                case "mongoload":
                    handleMongoLoadCommand(command);
                    break;
                    
                case "clean":
                    handleCleanCommand(command);
                    break;
                    
                case "retry":
                    handleRetryCommand();
                    break;
                    
                case "help":
                    MenuHandler.printUsage();
                    break;
                    
                case "unknown":
                default:
                    System.err.println("错误: 未知命令 '" + (args.length > 0 ? args[0] : "") + "'");
                    MenuHandler.printUsage();
                    System.exit(1);
            }
            
        } catch (Exception e) {
            System.err.println("\n程序运行出错: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static void handleRowKeyCommand(CommandLineParser.Command command) {
        String rowKey = command.getParameter("rowkey");
        if (rowKey == null || rowKey.isEmpty()) {
            System.err.println("错误: rowkey命令需要行键参数");
            System.err.println("用法: rowkey <行键>");
            System.exit(1);
        }
        new SearchService().executeRowKeySearch(rowKey);
    }
    
    private static void handleStatsCommand(CommandLineParser.Command command) {
        String startTime = command.getParameter("startTime", "00:00:00");
        String endTime = command.getParameter("endTime", "23:59:59");
        String outputDir = command.getParameter("outputDir", "output");
        String zkQuorum = command.getParameter("zkQuorum", "localhost");
        String zkPort = command.getParameter("zkPort", "2181");
        boolean generateCharts = command.getBooleanParameter("generateCharts", true);
        
        new SparkAnalysisService().executeSparkAnalysisCommandLine(
            startTime, endTime, outputDir, zkQuorum, zkPort, generateCharts);
    }
    
    private static void handleLoadCommand(CommandLineParser.Command command) {
        String filePath = command.getParameter("filePath");
        if (filePath == null || filePath.isEmpty()) {
            System.err.println("错误: load命令需要数据文件参数");
            System.err.println("用法: load <数据文件> [ZooKeeper地址] [ZooKeeper端口] [批量大小] [编码]");
            System.err.println("示例: load log.tsv localhost 2181 5000 UTF-8");
            System.exit(1);
        }
        
        String zkQuorum = command.getParameter("zkQuorum", "localhost");
        String zkPort = command.getParameter("zkPort", "2181");
        int batchSize = command.getIntParameter("batchSize", 5000);
        String encoding = command.getParameter("encoding", "UTF-8");
        
        new DataLoadService().executeHBaseDataLoad(filePath, zkQuorum, zkPort, batchSize, encoding);
    }
    
    private static void handleMongoLoadCommand(CommandLineParser.Command command) {
        String filePath = command.getParameter("filePath");
        if (filePath == null || filePath.isEmpty()) {
            System.err.println("错误: mongoload命令需要数据文件参数");
            System.err.println("用法: mongoload <数据文件> [MongoDB连接字符串] [批量大小] [编码]");
            System.err.println("示例: mongoload log.tsv \"mongodb://127.0.0.1:27017/\" 5000 UTF-8");
            System.exit(1);
        }
        
        String connectionString = command.getParameter("connectionString", 
            "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.5.10");
        int batchSize = command.getIntParameter("batchSize", 5000);
        String encoding = command.getParameter("encoding", "UTF-8");
        
        new DataLoadService().executeMongoDataLoad(filePath, connectionString, batchSize, encoding);
    }
    
    private static void handleCleanCommand(CommandLineParser.Command command) {
        String operation = command.getParameter("operation", "");
        if (!operation.isEmpty() && !operation.equals("truncate")) {
            System.err.println("错误: clean 命令只支持清空表操作");
            System.err.println("用法: clean 或 clean truncate");
            System.exit(1);
        }
        
        new DataLoadService().executeCleanTableCommandLine();
    }
    
    private static void handleRetryCommand() {
        new MenuHandler().retryIncompleteTasks();
    }
    
    private static void handleSparkSearchCommand(CommandLineParser.Command command) {
        String conditionStr = command.getParameter("condition");
        if (conditionStr == null || conditionStr.isEmpty()) {
            System.err.println("错误: sparksearch命令需要搜索条件参数");
            System.err.println("用法: sparksearch <搜索条件> [ZooKeeper地址] [ZooKeeper端口] [是否显示详情]");
            System.err.println("示例: sparksearch \"time:00:00:00|01:00:00 + user:user1\" localhost 2181 true");
            System.err.println("搜索条件格式: time:起始时间|结束时间 + user:用户1|用户2 + query:关键词 + domain:域名 + rank:最小-最大 + click:最小-最大");
            System.exit(1);
        }
        
        String zkQuorum = command.getParameter("zkQuorum", "localhost");
        String zkPort = command.getParameter("zkPort", "2181");
        boolean showDetails = command.getBooleanParameter("showDetails", true);
        
        com.sohu.logs.search.SearchCondition condition = com.sohu.logs.search.SearchCondition.parse(conditionStr);
        new com.sohu.logs.service.SparkSearchService().executeSparkSearchCommandLine(condition, zkQuorum, zkPort, showDetails);
    }
}