package com.sohu.logs.util;

import java.time.LocalDateTime;

public class TimeUtils {
    
    public static long parseTimeOnly(String timeStr) {
        String trimmed = timeStr.trim();
        String[] parts = trimmed.split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("时间格式错误，应为HH:mm:ss: " + trimmed);
        }
        int hours = Integer.parseInt(parts[0]);
        int minutes = Integer.parseInt(parts[1]);
        int seconds = Integer.parseInt(parts[2]);
        
        return ((hours * 60L + minutes) * 60L + seconds) * 1000L;
    }
    
    public static LocalDateTime parseTimeOnlyToDateTime(String timeStr) {
        long timestamp = parseTimeOnly(timeStr);
        return LocalDateTime.ofEpochSecond(timestamp / 1000, 0, java.time.ZoneOffset.UTC);
    }

    public static String toReverseTimestampStr(long timestamp) {
        long reverse = Long.MAX_VALUE - timestamp;
        return String.format("%019d", reverse);
    }
}