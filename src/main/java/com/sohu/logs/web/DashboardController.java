package com.sohu.logs.web;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.http.staticfiles.Location;
import com.sohu.logs.service.*;
import com.sohu.logs.search.SearchCondition;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jspecify.annotations.NonNull;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.UUID;

/**
 * Dashboard web controller for the search log analyzer
 */
public class DashboardController {

    // Inner class for task management
    public static class Task {
        private String id;
        private String type;
        private String status; // pending, running, completed, failed
        private String progress;
        private String result;
        private StringBuilder output;
        private long createdAt;
        private long updatedAt;

        public Task(String id, String type) {
            this.id = id;
            this.type = type;
            this.status = "pending";
            this.output = new StringBuilder();
            this.createdAt = System.currentTimeMillis();
            this.updatedAt = this.createdAt;
        }

        // Getters and setters
        public String getId() { return id; }
        public String getType() { return type; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; this.updatedAt = System.currentTimeMillis(); }
        public String getProgress() { return progress; }
        public void setProgress(String progress) { this.progress = progress; }
        public String getResult() { return result; }
        public void setResult(String result) { this.result = result; }
        public String getOutput() { return output.toString(); }
        public void appendOutput(String text) { output.append(text); }
        public long getCreatedAt() { return createdAt; }
        public long getUpdatedAt() { return updatedAt; }
    }

    /**
     * Utility class for capturing console output during synchronous operations
     */
    private static class ConsoleOutputCapture implements AutoCloseable {
        private final java.io.PrintStream originalOut;
        private final java.io.PrintStream originalErr;
        private final java.io.ByteArrayOutputStream buffer;
        private final java.io.PrintStream outTeeStream;
        private final java.io.PrintStream errTeeStream;

        public ConsoleOutputCapture() {
            this.originalOut = System.out;
            this.originalErr = System.err;
            this.buffer = new java.io.ByteArrayOutputStream();

            this.outTeeStream = new java.io.PrintStream(new java.io.OutputStream() {
                @Override
                public void write(int b) {
                    originalOut.write(b);
                    buffer.write(b);
                }
                
                @Override
                public void write(byte @NonNull [] b, int off, int len) {
                    originalOut.write(b, off, len);
                    buffer.write(b, off, len);
                }
                
                @Override
                public void flush() {
                    originalOut.flush();
                }
            });
            
            // Create tee output stream for System.err
            this.errTeeStream = new java.io.PrintStream(new java.io.OutputStream() {
                @Override
                public void write(int b) {
                    originalErr.write(b);
                    buffer.write(b);
                }
                
                @Override
                public void write(byte @NonNull [] b, int off, int len) {
                    originalErr.write(b, off, len);
                    buffer.write(b, off, len);
                }
                
                @Override
                public void flush() {
                    originalErr.flush();
                }
            });
            
            System.setOut(outTeeStream);
            System.setErr(errTeeStream);
        }
        
        public String getCapturedOutput() {
            outTeeStream.flush();
            errTeeStream.flush();
            return buffer.toString();
        }
        
        @Override
        public void close() {
            System.setOut(originalOut);
            System.setErr(originalErr);
            outTeeStream.close();
            errTeeStream.close();
        }
    }

    private final Javalin app;
    private final ObjectMapper objectMapper;
    private final SearchService searchService;
    private final SparkAnalysisService analysisService;
    private final DataLoadService dataLoadService;
    private final SparkSearchService sparkSearchService;

    // Task management
    private final ConcurrentHashMap<String, Task> tasks = new ConcurrentHashMap<>();
    private final AtomicLong taskIdGenerator = new AtomicLong(1);

    public DashboardController(int port) {
        this.app = Javalin.create(config -> {
            String staticDir = new File("build/resources/main/static").getAbsolutePath();
            config.addStaticFiles("/static", staticDir, Location.EXTERNAL);
        });
        this.objectMapper = new ObjectMapper();
        this.searchService = new SearchService();
        this.analysisService = new SparkAnalysisService();
        this.dataLoadService = new DataLoadService();
        this.sparkSearchService = new SparkSearchService();

        setupRoutes();
        app.start(port);
    }

    private void setupRoutes() {
        // Home page
        app.get("/", this::homePage);
        app.get("/hbase", this::hbasePage);
        app.get("/spark", this::sparkPage);

        // API endpoints
        app.get("/api/status", this::getStatus);
        app.post("/api/shutdown", this::shutdown);

        // Search endpoints
        app.post("/api/search/rowkey", this::searchByRowKey);
        app.post("/api/search/condition", this::searchByCondition);
        app.post("/api/search/spark", this::sparkSearch);

        // Analysis endpoints
        app.post("/api/analysis/stats", this::runAnalysis);

        // Data management endpoints
        app.post("/api/data/load", this::loadData);
        app.post("/api/data/clean", this::cleanData);

        // Task management endpoints
        app.get("/api/task/status/:taskId", this::getTaskStatus);

        // Monitoring endpoints
        app.get("/api/monitor/system", this::getSystemMonitor);
        
        // Recent operations endpoint
        app.get("/api/recent/operations", this::getRecentOperations);
    }

    private void homePage(Context ctx) {
        try {
            // Read the static HTML file from resources
            java.io.InputStream inputStream = getClass().getClassLoader().getResourceAsStream("static/index.html");
            if (inputStream != null) {
                java.io.ByteArrayOutputStream result = new java.io.ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int length;
                while ((length = inputStream.read(buffer)) != -1) {
                    result.write(buffer, 0, length);
                }
                String html = result.toString("UTF-8");
                ctx.html(html);
            } else {
                ctx.result("Dashboard HTML file not found").status(404);
            }
        } catch (Exception e) {
            ctx.result("Error loading dashboard: " + (e.getMessage() != null ? e.getMessage() : e.toString())).status(500);
        }
    }

    private void hbasePage(Context ctx) {
        try {
            java.io.InputStream inputStream = getClass().getClassLoader().getResourceAsStream("static/hbase.html");
            if (inputStream != null) {
                java.io.ByteArrayOutputStream result = new java.io.ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int length;
                while ((length = inputStream.read(buffer)) != -1) {
                    result.write(buffer, 0, length);
                }
                String html = result.toString("UTF-8");
                ctx.html(html);
            } else {
                ctx.result("HBase HTML file not found").status(404);
            }
        } catch (Exception e) {
            ctx.result("Error loading HBase page: " + (e.getMessage() != null ? e.getMessage() : e.toString())).status(500);
        }
    }

    private void sparkPage(Context ctx) {
        try {
            java.io.InputStream inputStream = getClass().getClassLoader().getResourceAsStream("static/spark.html");
            if (inputStream != null) {
                java.io.ByteArrayOutputStream result = new java.io.ByteArrayOutputStream();
                byte[] buffer = new byte[1024];
                int length;
                while ((length = inputStream.read(buffer)) != -1) {
                    result.write(buffer, 0, length);
                }
                String html = result.toString("UTF-8");
                ctx.html(html);
            } else {
                ctx.result("Spark HTML file not found").status(404);
            }
        } catch (Exception e) {
            ctx.result("Error loading Spark page: " + (e.getMessage() != null ? e.getMessage() : e.toString())).status(500);
        }
    }

    private void getStatus(Context ctx) {
        ctx.result("系统运行正常 - " + new java.util.Date());
    }

    private void shutdown(Context ctx) {
        try {
            ctx.result("正在关闭Dashboard服务器...");
            // Stop the server after a short delay to allow response to be sent
            new Thread(() -> {
                try {
                    Thread.sleep(1000);
                    // Cleanup Redis connections
                    cleanup();
                    stop();
                    System.exit(0);
                } catch (InterruptedException e) {
                    System.err.println("关闭服务器时出错: " + e.getMessage());
                }
            }).start();
        } catch (Exception e) {
            ctx.status(500).result("错误: " + e.getMessage());
        }
    }

    private void cleanup() {
        try {
            searchService.cleanup();
            dataLoadService.cleanup();
            // Spark services don't have Redis connections to cleanup
        } catch (Exception e) {
            System.err.println("清理资源时出错: " + e.getMessage());
        }
    }

    private void searchByRowKey(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        try (ConsoleOutputCapture capture = new ConsoleOutputCapture()) {
            Map<String, String> request = objectMapper.readValue(ctx.body(), Map.class);
            String rowkey = request.get("rowkey");
            if (rowkey == null || rowkey.isEmpty()) {
                response.put("success", false);
                response.put("error", "行键不能为空");
                ctx.status(400).json(response);
                return;
            }
            Map<String, Object> result = searchService.searchRowKeyAsMap(rowkey);
            response.put("success", true);
            if (result == null) {
                response.put("message", "未找到行键对应的记录");
                response.put("found", false);
                response.put("data", "未找到行键对应的记录: " + rowkey);
            } else {
                response.put("message", "查询成功");
                response.put("found", true);
                response.put("result", result);
                // 构建格式化输出
                StringBuilder sb = new StringBuilder();
                sb.append("=== HBase行键精确查询 ===\n");
                sb.append("查询行键: ").append(rowkey).append("\n");
                sb.append("行键: ").append(result.get("rowKey")).append("\n");
                sb.append("  时间: ").append(result.get("accessTime")).append("\n");
                sb.append("  用户: ").append(result.get("userId")).append("\n");
                sb.append("  查询词: ").append(result.get("query")).append("\n");
                sb.append("  排名: ").append(result.get("rank")).append("\n");
                sb.append("  点击顺序: ").append(result.get("clickOrder")).append("\n");
                sb.append("  URL: ").append(result.get("url")).append("\n");
                sb.append("  域名: ").append(result.get("domain")).append("\n");
                response.put("data", sb.toString());
            }
            // 添加控制台输出到响应
            String consoleOutput = capture.getCapturedOutput();
            if (consoleOutput != null && !consoleOutput.isEmpty()) {
                response.put("consoleOutput", consoleOutput);
            }
            ctx.json(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            ctx.status(500).json(response);
        }
    }

    private void searchByCondition(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        try (ConsoleOutputCapture capture = new ConsoleOutputCapture()) {
            Map<String, String> request = objectMapper.readValue(ctx.body(), Map.class);
            String conditionStr = request.get("condition");
            if (conditionStr == null || conditionStr.isEmpty()) {
                response.put("success", false);
                response.put("error", "搜索条件不能为空");
                ctx.status(400).json(response);
                return;
            }
            SearchCondition condition = SearchCondition.parse(conditionStr);
            response.put("condition", condition.toString());
            List<Map<String, Object>> results = searchService.searchConditionAsList(condition);
            response.put("success", true);
            response.put("count", results.size());
            if (results.isEmpty()) {
                response.put("message", "未找到符合条件的记录");
                response.put("data", "未找到符合条件的记录");
            } else {
                String message = "查询成功，找到 " + results.size() + " 条记录";
                response.put("message", message);
                StringBuilder sb = new StringBuilder();
                sb.append("=== HBase条件查询 ===\n");
                sb.append("查询条件: ").append(condition).append("\n");
                sb.append("找到 ").append(results.size()).append(" 条记录\n");
                if (!results.isEmpty()) {
                    sb.append("\n前").append(Math.min(10, results.size())).append("条记录:\n");
                    int limit = Math.min(10, results.size());
                    for (int i = 0; i < limit; i++) {
                        Map<String, Object> record = results.get(i);
                        sb.append("行键: ").append(record.get("rowKey")).append("\n");
                        sb.append("  时间: ").append(record.get("accessTime")).append("\n");
                        sb.append("  用户: ").append(record.get("userId")).append("\n");
                        sb.append("  查询词: ").append(record.get("query")).append("\n");
                        sb.append("  排名: ").append(record.get("rank")).append("\n");
                        sb.append("  点击顺序: ").append(record.get("clickOrder")).append("\n");
                        sb.append("  URL: ").append(record.get("url")).append("\n");
                        sb.append("  域名: ").append(record.get("domain")).append("\n");
                    }
                    if (results.size() > 10) {
                        sb.append("... 还有 ").append(results.size() - 10).append(" 条记录\n");
                    }
                }
                response.put("data", sb.toString());
            }
            response.put("results", results);
            // 添加控制台输出到响应
            String consoleOutput = capture.getCapturedOutput();
            if (consoleOutput != null && !consoleOutput.isEmpty()) {
                response.put("consoleOutput", consoleOutput);
            }
            ctx.json(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            ctx.status(500).json(response);
        }
    }

    private void sparkSearch(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        try (ConsoleOutputCapture capture = new ConsoleOutputCapture()) {
            Map<String, String> request = objectMapper.readValue(ctx.body(), Map.class);
            String conditionStr = request.get("condition");
            if (conditionStr == null || conditionStr.isEmpty()) {
                response.put("success", false);
                response.put("error", "搜索条件不能为空");
                ctx.status(400).json(response);
                return;
            }
            String zkQuorum = request.getOrDefault("zkQuorum", "localhost");
            String zkPort = request.getOrDefault("zkPort", "2181");
            SearchCondition condition = SearchCondition.parse(conditionStr);
            List<Map<String, Object>> results = sparkSearchService.executeSparkSearchAsList(condition, zkQuorum, zkPort);
            response.put("condition", condition.toString());
            response.put("success", true);
            response.put("count", results.size());
            if (results.isEmpty()) {
                response.put("message", "未找到符合条件的记录");
                response.put("data", "未找到符合条件的记录");
            } else {
                String message = "查询成功，找到 " + results.size() + " 条记录";
                response.put("message", message);
                StringBuilder sb = new StringBuilder();
                sb.append("=== Spark条件查询 ===\n");
                sb.append("查询条件: ").append(condition).append("\n");
                sb.append("找到 ").append(results.size()).append(" 条记录\n");
                if (!results.isEmpty()) {
                    sb.append("\n前").append(Math.min(10, results.size())).append("条记录:\n");
                    int limit = Math.min(10, results.size());
                    for (int i = 0; i < limit; i++) {
                        Map<String, Object> record = results.get(i);
                        sb.append("时间: ").append(record.get("accessTime")).append("\n");
                        sb.append("  用户: ").append(record.get("userId")).append("\n");
                        sb.append("  查询词: ").append(record.get("query")).append("\n");
                        sb.append("  排名: ").append(record.get("rank")).append("\n");
                        sb.append("  点击顺序: ").append(record.get("clickOrder")).append("\n");
                        sb.append("  URL: ").append(record.get("url")).append("\n");
                        sb.append("  域名: ").append(record.get("domain")).append("\n");
                    }
                    if (results.size() > 10) {
                        sb.append("... 还有 ").append(results.size() - 10).append(" 条记录\n");
                    }
                }
                response.put("data", sb.toString());
            }
            response.put("results", results);
            // 添加控制台输出到响应
            String consoleOutput = capture.getCapturedOutput();
            if (consoleOutput != null && !consoleOutput.isEmpty()) {
                response.put("consoleOutput", consoleOutput);
            }
            ctx.json(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            ctx.status(500).json(response);
        }
    }

    private void runAnalysis(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        try {
            Map<String, String> request = objectMapper.readValue(ctx.body(), Map.class);
            String startTime = request.getOrDefault("startTime", "00:00:00");
            String endTime = request.getOrDefault("endTime", "23:59:59");
            String outputDir = request.getOrDefault("outputDir", "output");
            String zkQuorum = request.getOrDefault("zkQuorum", "localhost");
            String zkPort = request.getOrDefault("zkPort", "2181");

            // Create async task
            String taskId = UUID.randomUUID().toString();
            Task task = new Task(taskId, "analysis");
            tasks.put(taskId, task);

            // Run in background
            new Thread(() -> {
                task.setStatus("running");
                task.setProgress("开始分析...");
                try {
                    java.io.PrintStream oldOut = System.out;
                    java.io.PrintStream oldErr = System.err;
                    java.io.PrintStream teePrintStream = new java.io.PrintStream(new java.io.OutputStream() {
                        @Override
                        public void write(int b) {
                            oldOut.write(b);
                            task.appendOutput(String.valueOf((char)b));
                        }
                        @Override
                        public void write(byte @NonNull [] b, int off, int len) {
                            oldOut.write(b, off, len);
                            task.appendOutput(new String(b, off, len));
                        }
                    });
                    System.setOut(teePrintStream);
                    System.setErr(teePrintStream);
                    try {
                        task.setProgress("执行Spark分析...");
                        analysisService.executeSparkAnalysisCommandLine(startTime, endTime, outputDir, zkQuorum, zkPort, true);
                        String timestamp = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                        String baseOutputDir = outputDir + "/" + timestamp;
                        String resultMessage = String.format("分析完成，结果保存在: %s (时间范围: %s 至 %s)", 
                            baseOutputDir, startTime, endTime);
                        task.setResult(resultMessage);
                        task.setStatus("completed");
                        task.setProgress("100%");
                    } finally {
                        System.setOut(oldOut);
                        System.setErr(oldErr);
                    }
                } catch (Exception e) {
                    task.setStatus("failed");
                    task.setResult("错误: " + e.getMessage());
                }
            }).start();

            response.put("success", true);
            response.put("taskId", taskId);
            response.put("message", "分析任务已启动，请查询任务状态");
            ctx.json(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            ctx.status(500).json(response);
        }
    }

    private void loadData(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        try {
            Map<String, String> request = objectMapper.readValue(ctx.body(), Map.class);
            String filePath = request.get("filePath");
            String zkQuorum = request.getOrDefault("zkQuorum", "localhost");
            String zkPort = request.getOrDefault("zkPort", "2181");

            if (filePath == null || filePath.isEmpty()) {
                response.put("success", false);
                response.put("error", "数据文件路径不能为空");
                ctx.status(400).json(response);
                return;
            }

            // Create async task
            String taskId = UUID.randomUUID().toString();
            Task task = new Task(taskId, "data_load");
            tasks.put(taskId, task);

            // Run in background
            new Thread(() -> {
                task.setStatus("running");
                task.setProgress("开始加载数据...");
                try {
                    java.io.PrintStream oldOut = System.out;
                    java.io.PrintStream oldErr = System.err;
                    java.io.PrintStream teePrintStream = new java.io.PrintStream(new java.io.OutputStream() {
                        @Override
                        public void write(int b) {
                            oldOut.write(b);
                            task.appendOutput(String.valueOf((char)b));
                        }
                        @Override
                        public void write(byte @NonNull [] b, int off, int len) {
                            oldOut.write(b, off, len);
                            task.appendOutput(new String(b, off, len));
                        }
                    });
                    System.setOut(teePrintStream);
                    System.setErr(teePrintStream);
                    try {
                        task.setProgress("执行数据加载...");
                        dataLoadService.executeHBaseDataLoad(filePath, zkQuorum, zkPort, 5000, "UTF-8");
                        String resultMessage = String.format("数据加载完成，文件: %s (ZooKeeper: %s:%s)", 
                            filePath, zkQuorum, zkPort);
                        task.setResult(resultMessage);
                        task.setStatus("completed");
                        task.setProgress("100%");
                    } finally {
                        System.setOut(oldOut);
                        System.setErr(oldErr);
                    }
                } catch (Exception e) {
                    task.setStatus("failed");
                    task.setResult("错误: " + e.getMessage());
                }
            }).start();

            response.put("success", true);
            response.put("taskId", taskId);
            response.put("message", "数据加载任务已启动，请查询任务状态");
            ctx.json(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            ctx.status(500).json(response);
        }
    }

    private void cleanData(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        try {
            // Create async task
            String taskId = UUID.randomUUID().toString();
            Task task = new Task(taskId, "data_clean");
            tasks.put(taskId, task);

            // Run in background
            new Thread(() -> {
                task.setStatus("running");
                task.setProgress("开始清空数据表...");
                try {
                    java.io.PrintStream oldOut = System.out;
                    java.io.PrintStream oldErr = System.err;
                    java.io.PrintStream teePrintStream = new java.io.PrintStream(new java.io.OutputStream() {
                        @Override
                        public void write(int b) {
                            oldOut.write(b);
                            task.appendOutput(String.valueOf((char)b));
                        }
                        @Override
                        public void write(byte @NonNull [] b, int off, int len) {
                            oldOut.write(b, off, len);
                            task.appendOutput(new String(b, off, len));
                        }
                    });
                    System.setOut(teePrintStream);
                    System.setErr(teePrintStream);
                    try {
                        task.setProgress("执行清空操作...");
                        dataLoadService.executeCleanTableCommandLine();
                        task.setResult("数据表清空完成");
                        task.setStatus("completed");
                        task.setProgress("100%");
                    } finally {
                        System.setOut(oldOut);
                        System.setErr(oldErr);
                    }
                } catch (Exception e) {
                    task.setStatus("failed");
                    task.setResult("错误: " + e.getMessage());
                }
            }).start();

            response.put("success", true);
            response.put("taskId", taskId);
            response.put("message", "清空任务已启动，请查询任务状态");
            ctx.json(response);
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            ctx.status(500).json(response);
        }
    }

    public void stop() {
        if (app != null) {
            app.stop();
        }
    }

    private void getTaskStatus(Context ctx) {
        String taskId = ctx.pathParam("taskId");
        Task task = tasks.get(taskId);
        if (task == null) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", "任务不存在");
            ctx.status(404).json(response);
            return;
        }
        ctx.json(task);
    }

    private void getSystemMonitor(Context ctx) {
        Map<String, Object> monitor = new HashMap<>();
        try {
            Runtime runtime = Runtime.getRuntime();
            long totalMemory = runtime.totalMemory();
            long freeMemory = runtime.freeMemory();
            long usedMemory = totalMemory - freeMemory;
            int availableProcessors = runtime.availableProcessors();

            // Simple CPU usage (this is approximate)
            double cpuUsage = Math.random() * 100; // Placeholder, in real app use JMX or OS tools

            monitor.put("cpuUsage", String.format("%.1f", cpuUsage) + "%");
            monitor.put("totalMemory", totalMemory / 1024 / 1024 + " MB");
            monitor.put("usedMemory", usedMemory / 1024 / 1024 + " MB");
            monitor.put("freeMemory", freeMemory / 1024 / 1024 + " MB");
            monitor.put("availableProcessors", availableProcessors);
            monitor.put("activeTasks", tasks.size());

            ctx.json(monitor);
        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("error", e.getMessage());
            ctx.status(500).json(response);
        }
    }

    private void getRecentOperations(Context ctx) {
        Map<String, Object> response = new HashMap<>();
        try {
            Path logsDir = Paths.get("task_logs");
            if (!Files.exists(logsDir) || !Files.isDirectory(logsDir)) {
                response.put("success", false);
                response.put("error", "task_logs目录不存在");
                ctx.status(404).json(response);
                return;
            }

            List<String> allLogEntries = new ArrayList<>();
            
            // 读取所有日志文件
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(logsDir, "*.log")) {
                for (Path file : stream) {
                    try {
                        List<String> lines = Files.readAllLines(file);
                        allLogEntries.addAll(lines);
                    } catch (Exception e) {
                        System.err.println("读取日志文件失败: " + file + " - " + e.getMessage());
                    }
                }
            }

            // 按时间戳排序（日志格式：2026-01-09 10:36:52|...）
            allLogEntries.sort((a, b) -> {
                try {
                    String timeA = a.split("\\|")[0];
                    String timeB = b.split("\\|")[0];
                    return timeB.compareTo(timeA); // 降序，最新的在前
                } catch (Exception e) {
                    return 0;
                }
            });

            // 只返回最近10条记录
            int limit = Math.min(10, allLogEntries.size());
            List<String> recentOps = allLogEntries.subList(0, limit);
            
            // 格式化输出
            List<Map<String, String>> formattedOps = new ArrayList<>();
            for (String log : recentOps) {
                String[] parts = log.split("\\|", 5);
                if (parts.length >= 5) {
                    Map<String, String> op = new HashMap<>();
                    op.put("timestamp", parts[0]);
                    op.put("taskId", parts[1]);
                    op.put("status", parts[2]);
                    op.put("type", parts[3]);
                    op.put("details", parts[4]);
                    formattedOps.add(op);
                } else if (parts.length >= 4) {
                    Map<String, String> op = new HashMap<>();
                    op.put("timestamp", parts[0]);
                    op.put("taskId", parts[1]);
                    op.put("status", parts[2]);
                    op.put("type", parts[3]);
                    op.put("details", "");
                    formattedOps.add(op);
                }
            }

            response.put("success", true);
            response.put("operations", formattedOps);
            ctx.json(response);
            
        } catch (Exception e) {
            response.put("success", false);
            response.put("error", e.getMessage());
            ctx.status(500).json(response);
        }
    }
}