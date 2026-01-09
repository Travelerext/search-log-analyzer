package com.sohu.logs.cli;

import java.util.HashMap;
import java.util.Map;

public class CommandLineParser {
    
    public static Command parse(String[] args) {
        if (args.length == 0) {
            return new Command("menu", new HashMap<>());
        }
        
        String command = args[0].toLowerCase();
        Map<String, String> params = new HashMap<>();
        
        switch (command) {
            case "rowkey":
                if (args.length >= 2) {
                    params.put("rowkey", args[1]);
                }
                break;
                
            case "stats":
                if (args.length >= 2) params.put("startTime", args[1]);
                if (args.length >= 3) params.put("endTime", args[2]);
                if (args.length >= 4) params.put("outputDir", args[3]);
                if (args.length >= 5) params.put("zkQuorum", args[4]);
                if (args.length >= 6) params.put("zkPort", args[5]);
                if (args.length >= 7) params.put("generateCharts", args[6]);
                break;
                
            case "search":
                
                break;
                
            case "sparksearch":
                if (args.length >= 2) params.put("condition", args[1]);
                if (args.length >= 3) params.put("zkQuorum", args[2]);
                if (args.length >= 4) params.put("zkPort", args[3]);
                if (args.length >= 5) params.put("showDetails", args[4]);
                break;
                
            case "load":
                if (args.length >= 2) params.put("filePath", args[1]);
                if (args.length >= 3) params.put("zkQuorum", args[2]);
                if (args.length >= 4) params.put("zkPort", args[3]);
                if (args.length >= 5) params.put("batchSize", args[4]);
                if (args.length >= 6) params.put("encoding", args[5]);
                break;
                
             case "mongoload":
                 if (args.length >= 2) params.put("filePath", args[1]);
                 if (args.length >= 3) params.put("connectionString", args[2]);
                 if (args.length >= 4) params.put("batchSize", args[3]);
                 if (args.length >= 5) params.put("encoding", args[4]);
                 break;

             case "web":
                 if (args.length >= 2) params.put("port", args[1]);
                 break;

             case "retry":
                 if (args.length >= 2) params.put("taskId", args[1]);
                 break;

             case "help":
            case "-h":
            case "--help":
                return new Command("help", params);
                
            default:
                return new Command("unknown", params);
        }
        
        return new Command(command, params);
    }
    
    public static class Command {
        private final String name;
        private final Map<String, String> parameters;
        
        public Command(String name, Map<String, String> parameters) {
            this.name = name;
            this.parameters = parameters;
        }
        
        public String getName() {
            return name;
        }
        
        public String getParameter(String key) {
            return parameters.get(key);
        }
        
        public String getParameter(String key, String defaultValue) {
            return parameters.getOrDefault(key, defaultValue);
        }
        
        public int getIntParameter(String key, int defaultValue) {
            try {
                return Integer.parseInt(parameters.get(key));
            } catch (NumberFormatException | NullPointerException e) {
                return defaultValue;
            }
        }
        
        public boolean getBooleanParameter(String key, boolean defaultValue) {
            try {
                String value = parameters.get(key);
                if (value == null) {
                    return defaultValue;
                }
                if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("yes") || 
                    value.equalsIgnoreCase("y") || value.equalsIgnoreCase("1")) {
                    return true;
                }
                if (value.equalsIgnoreCase("false") || value.equalsIgnoreCase("no") || 
                    value.equalsIgnoreCase("n") || value.equalsIgnoreCase("0")) {
                    return false;
                }
                return defaultValue;
            } catch (Exception e) {
                return defaultValue;
            }
        }
    }
}