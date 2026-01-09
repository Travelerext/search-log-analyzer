package com.sohu.logs.search;

import com.sohu.logs.util.TimeUtils;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

public class SearchCondition {
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private long startTimeMillis;
    private long endTimeMillis;
    private List<String> userIds;
    private List<String> queryKeywords;
    private List<String> domainKeywords;
    private List<String> urlKeywords;
    private Integer minRank;
    private Integer maxRank;
    private Integer minClickOrder;
    private Integer maxClickOrder;

    public SearchCondition() {}
    
    public static SearchCondition parse(String input) {
        SearchCondition condition = new SearchCondition();
        if (input == null || input.trim().isEmpty()) {
            return condition;
        }
        
        String[] parts = input.split("\\+");
        for (String part : parts) {
            parseConditionPart(part.trim(), condition);
        }
        return condition;
    }
    
    private static void parseConditionPart(String part, SearchCondition condition) {
        if (part.startsWith("time:")) {
            parseTimeRange(part.substring(5), condition);
        } else if (part.startsWith("user:")) {
            parseUserIds(part.substring(5), condition);
        } else if (part.startsWith("query:")) {
            parseQueryKeywords(part.substring(6), condition);
        } else if (part.startsWith("domain:")) {
            parseDomainKeywords(part.substring(7), condition);
        } else if (part.startsWith("url:")) {
            parseUrlKeywords(part.substring(4), condition);
        } else if (part.startsWith("rank:")) {
            parseRankRange(part.substring(5), condition);
        } else if (part.startsWith("click:")) {
            parseClickOrderRange(part.substring(6), condition);
        }
    }
    
    private static void parseTimeRange(String timeStr, SearchCondition condition) {
        String[] times = timeStr.split("\\|");
        if (times.length == 2) {
            condition.startTime = TimeUtils.parseTimeOnlyToDateTime(times[0].trim());
            condition.endTime = TimeUtils.parseTimeOnlyToDateTime(times[1].trim());
            condition.startTimeMillis = TimeUtils.parseTimeOnly(times[0].trim());
            condition.endTimeMillis = TimeUtils.parseTimeOnly(times[1].trim());
        }
    }
    
    private static void parseUserIds(String userIdStr, SearchCondition condition) {
        condition.userIds = Arrays.asList(userIdStr.split("\\|"));
    }
    
    private static void parseQueryKeywords(String queryStr, SearchCondition condition) {
        condition.queryKeywords = Arrays.asList(queryStr.split("\\|"));
    }
    
    private static void parseDomainKeywords(String domainStr, SearchCondition condition) {
        condition.domainKeywords = Arrays.asList(domainStr.split("\\|"));
    }

    private static void parseUrlKeywords(String urlStr, SearchCondition condition) {
        condition.urlKeywords = Arrays.asList(urlStr.split("\\|"));
    }
    
    private static void parseRankRange(String rankStr, SearchCondition condition) {
        try {
            if (rankStr.contains("-")) {
                String[] range = rankStr.split("-");
                if (range.length == 2) {
                    condition.minRank = Integer.parseInt(range[0].trim());
                    condition.maxRank = Integer.parseInt(range[1].trim());
                }
            } else {
                
                int value = Integer.parseInt(rankStr.trim());
                condition.minRank = value;
                condition.maxRank = value;
            }
        } catch (NumberFormatException e) {
            System.err.println("警告: 排名范围格式错误，忽略此条件: " + rankStr);
        }
    }
    
    private static void parseClickOrderRange(String clickStr, SearchCondition condition) {
        try {
            if (clickStr.contains("-")) {
                String[] range = clickStr.split("-");
                if (range.length == 2) {
                    condition.minClickOrder = Integer.parseInt(range[0].trim());
                    condition.maxClickOrder = Integer.parseInt(range[1].trim());
                }
            } else {
                
                int value = Integer.parseInt(clickStr.trim());
                condition.minClickOrder = value;
                condition.maxClickOrder = value;
            }
        } catch (NumberFormatException e) {
            System.err.println("警告: 点击顺序范围格式错误，忽略此条件: " + clickStr);
        }
    }
    
    public boolean hasTimeRange() {
        return startTime != null && endTime != null;
    }
    
    public boolean hasUserIds() {
        return userIds != null && !userIds.isEmpty();
    }
    
    public boolean hasQueryKeywords() {
        return queryKeywords != null && !queryKeywords.isEmpty();
    }
    
    public boolean hasDomainKeywords() {
        return domainKeywords != null && !domainKeywords.isEmpty();
    }

    public boolean hasUrlKeywords() {
        return urlKeywords != null && !urlKeywords.isEmpty();
    }
    
    public boolean hasRankRange() {
        return minRank != null && maxRank != null;
    }
    
    public boolean hasClickOrderRange() {
        return minClickOrder != null && maxClickOrder != null;
    }
    
    public LocalDateTime getStartTime() {
        return startTime;
    }
    
    public LocalDateTime getEndTime() {
        return endTime;
    }
    
    public long getStartTimeMillis() {
        return startTimeMillis;
    }
    
    public long getEndTimeMillis() {
        return endTimeMillis;
    }
    
    public List<String> getUserIds() {
        return userIds != null ? userIds : new ArrayList<>();
    }
    
    public List<String> getQueryKeywords() {
        return queryKeywords != null ? queryKeywords : new ArrayList<>();
    }
    
    public List<String> getDomainKeywords() {
        return domainKeywords != null ? domainKeywords : new ArrayList<>();
    }

    public List<String> getUrlKeywords() {
        return urlKeywords != null ? urlKeywords : new ArrayList<>();
    }
    
    public Integer getMinRank() {
        return minRank;
    }
    
    public Integer getMaxRank() {
        return maxRank;
    }
    
    public Integer getMinClickOrder() {
        return minClickOrder;
    }
    
    public Integer getMaxClickOrder() {
        return maxClickOrder;
    }
    
    public String getReverseStartTimeStr() {
        long reverse = Long.MAX_VALUE - getEndTimeMillis();
        return String.format("%019d", reverse);
    }
    
    public String getReverseEndTimeStr() {
        long reverse = Long.MAX_VALUE - getStartTimeMillis();
        return String.format("%019d", reverse);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (hasTimeRange()) {
            sb.append("时间: ").append(startTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"))).append(" 至 ").append(endTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"))).append("; ");
        }
        if (hasUserIds()) {
            sb.append("用户: ").append(userIds).append("; ");
        }
        if (hasQueryKeywords()) {
            sb.append("查询关键词: ").append(queryKeywords).append("; ");
        }
        if (hasDomainKeywords()) {
            sb.append("域名关键词: ").append(domainKeywords).append("; ");
        }
        if (hasUrlKeywords()) {
            sb.append("URL关键词: ").append(urlKeywords).append("; ");
        }
        if (hasRankRange()) {
            sb.append("排名范围: ").append(minRank).append("-").append(maxRank).append("; ");
        }
        if (hasClickOrderRange()) {
            sb.append("点击顺序范围: ").append(minClickOrder).append("-").append(maxClickOrder).append("; ");
        }
        return sb.toString();
    }
    
    public String toCacheKey() {
        StringBuilder sb = new StringBuilder();
        if (hasTimeRange()) {
            sb.append("time:").append(startTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss"))).append("|").append(endTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
        }
        if (hasUserIds()) {
            if (sb.length() > 0) sb.append("+");
            sb.append("user:").append(String.join("|", userIds));
        }
        if (hasQueryKeywords()) {
            if (sb.length() > 0) sb.append("+");
            sb.append("query:").append(String.join("|", queryKeywords));
        }
        if (hasDomainKeywords()) {
            if (sb.length() > 0) sb.append("+");
            sb.append("domain:").append(String.join("|", domainKeywords));
        }
        if (hasUrlKeywords()) {
            if (sb.length() > 0) sb.append("+");
            sb.append("url:").append(String.join("|", urlKeywords));
        }
        if (hasRankRange()) {
            if (sb.length() > 0) sb.append("+");
            sb.append("rank:").append(minRank).append("-").append(maxRank);
        }
        if (hasClickOrderRange()) {
            if (sb.length() > 0) sb.append("+");
            sb.append("click:").append(minClickOrder).append("-").append(maxClickOrder);
        }
        return sb.toString();
    }
}