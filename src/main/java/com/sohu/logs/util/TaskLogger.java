package com.sohu.logs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TaskLogger {
    private static final Logger log = LoggerFactory.getLogger(TaskLogger.class);
    private static final String LOG_DIR = "task_logs";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    static {
        
        try {
            Files.createDirectories(Paths.get(LOG_DIR));
        } catch (IOException e) {
            log.error("创建任务日志目录失败", e);
        }
    }
    
    public static String logTaskStart(String taskType, String taskId, String parameters) {
        if (taskId == null || taskId.isEmpty()) {
            taskId = generateTaskId(taskType);
        }
        
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format("%s|%s|START|%s|%s", 
                timestamp, taskId, taskType, parameters);
        
        writeLogEntry(logEntry);
        log.info("任务开始: {} - {}", taskId, taskType);
        
        return taskId;
    }
    
    public static void logTaskSuccess(String taskId, String taskType, String result) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format("%s|%s|SUCCESS|%s|%s", 
                timestamp, taskId, taskType, result);
        
        writeLogEntry(logEntry);
        log.info("任务成功: {} - {}", taskId, taskType);
    }
    
    public static void logTaskFailure(String taskId, String taskType, String error) {
        String timestamp = LocalDateTime.now().format(formatter);
        String logEntry = String.format("%s|%s|FAILURE|%s|%s", 
                timestamp, taskId, taskType, error);
        
        writeLogEntry(logEntry);
        log.error("任务失败: {} - {}: {}", taskId, taskType, error);
    }
    
    public static List<String> getIncompleteTasks() {
        List<String> incompleteTasks = new ArrayList<>();
        Path logFile = getLogFilePath();
        
        if (!Files.exists(logFile)) {
            return incompleteTasks;
        }
        
        try (BufferedReader reader = Files.newBufferedReader(logFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 4) {
                    String taskId = parts[1];
                    String status = parts[2];
                    
                    if ("START".equals(status)) {
                        
                        if (!hasCompletionRecord(taskId)) {
                            incompleteTasks.add(taskId);
                        }
                    }
                }
            }
        } catch (IOException e) {
            log.error("读取任务日志失败", e);
        }
        
        return incompleteTasks;
    }
    
    public static TaskInfo getTaskInfo(String taskId) {
        Path logFile = getLogFilePath();
        
        if (!Files.exists(logFile)) {
            return null;
        }
        
        try (BufferedReader reader = Files.newBufferedReader(logFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 5 && taskId.equals(parts[1]) && "START".equals(parts[2])) {
                    return new TaskInfo(parts[3], parts[4]); 
                }
            }
        } catch (IOException e) {
            log.error("读取任务日志失败", e);
        }
        
        return null;
    }
    
    private static boolean hasCompletionRecord(String taskId) {
        Path logFile = getLogFilePath();
        
        if (!Files.exists(logFile)) {
            return false;
        }
        
        try (BufferedReader reader = Files.newBufferedReader(logFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 4 && taskId.equals(parts[1]) && 
                    ("SUCCESS".equals(parts[2]) || "FAILURE".equals(parts[2]))) {
                    return true;
                }
            }
        } catch (IOException e) {
            log.error("读取任务日志失败", e);
        }
        
        return false;
    }
    
    private static void writeLogEntry(String logEntry) {
        Path logFile = getLogFilePath();
        
        try (BufferedWriter writer = Files.newBufferedWriter(logFile, 
                java.nio.file.StandardOpenOption.CREATE, 
                java.nio.file.StandardOpenOption.APPEND)) {
            writer.write(logEntry);
            writer.newLine();
        } catch (IOException e) {
            log.error("写入任务日志失败", e);
        }
    }
    
    private static Path getLogFilePath() {
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        return Paths.get(LOG_DIR, "tasks_" + date + ".log");
    }
    
    private static String generateTaskId(String taskType) {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        return taskType + "_" + timestamp + "_" + (int)(Math.random() * 1000);
    }
    
    public static class TaskInfo {
        private final String taskType;
        private final String parameters;
        
        public TaskInfo(String taskType, String parameters) {
            this.taskType = taskType;
            this.parameters = parameters;
        }
        
        public String getTaskType() {
            return taskType;
        }
        
        public String getParameters() {
            return parameters;
        }
    }
}