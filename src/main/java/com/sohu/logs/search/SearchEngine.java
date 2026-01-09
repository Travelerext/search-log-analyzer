package com.sohu.logs.search;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SearchEngine {
    private static final Logger log = LoggerFactory.getLogger(SearchEngine.class);
    private static final String TABLE_NAME = "search_logs";
    private static final byte[] CF = Bytes.toBytes("cf");
    private static final byte[] COL_TS = Bytes.toBytes("ts");
    private static final byte[] COL_USER = Bytes.toBytes("user");
    private static final byte[] COL_QUERY = Bytes.toBytes("query");
    private static final byte[] COL_RANK = Bytes.toBytes("rank");
    private static final byte[] COL_CLICK = Bytes.toBytes("click");
    private static final byte[] COL_URL = Bytes.toBytes("url");
    private static final byte[] COL_DOMAIN = Bytes.toBytes("domain");
    
    private final Connection connection;
    private final RedisCache redisCache;
    
    public SearchEngine(Connection connection) {
        this.connection = connection;
        this.redisCache = new RedisCache();
    }
    
    public SearchEngine(Connection connection, RedisCache redisCache) {
        this.connection = connection;
        this.redisCache = redisCache;
    }
    
    public List<Result> search(SearchCondition condition) throws IOException {
        
        String cacheKey = condition.toCacheKey();
        
        
        if (redisCache != null) {
            List<String> cachedRowKeys = redisCache.get(cacheKey);
            if (cachedRowKeys != null) {
                log.info("从Redis缓存获取搜索结果，条件: {}", cacheKey);
                return getResultsByRowKeys(cachedRowKeys);
            }
        }
        
        
        List<Result> results = performSearch(condition);
        
        
        if (results.size() > 100) {
            System.out.println("警告: 搜索结果超过100条，仅显示前100条");
            results = results.subList(0, 100);
        }
        
        
        if (redisCache != null && !results.isEmpty()) {
            List<String> rowKeys = new ArrayList<>();
            for (Result result : results) {
                rowKeys.add(Bytes.toString(result.getRow()));
            }
            redisCache.put(cacheKey, rowKeys);
            log.info("将搜索结果存入Redis缓存，条件: {}，结果数量: {}", cacheKey, rowKeys.size());
        }
        
        return results;
    }
    
    private List<Result> performSearch(SearchCondition condition) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Scan scan = new Scan();
            scan.setCaching(100);
            
            List<Filter> filters = new ArrayList<>();
            
            
            if (condition.hasTimeRange()) {
                setupTimeRangeScan(scan, condition);
                Filter timeFilter = createTimeFilter(condition);
                filters.add(timeFilter);
            }
            
            
            if (condition.hasUserIds()) {
                Filter userFilter = createUserFilter(condition.getUserIds());
                filters.add(userFilter);
            }
            
            
            if (condition.hasQueryKeywords()) {
                Filter queryFilter = createQueryFilter(condition.getQueryKeywords());
                filters.add(queryFilter);
            }
            
            
            if (condition.hasDomainKeywords()) {
                Filter domainFilter = createDomainFilter(condition.getDomainKeywords());
                filters.add(domainFilter);
            }

            if (condition.hasUrlKeywords()) {
                Filter urlFilter = createUrlFilter(condition.getUrlKeywords());
                filters.add(urlFilter);
            }

            
            if (condition.hasRankRange()) {
                Filter rankFilter = createRankFilter(condition.getMinRank(), condition.getMaxRank());
                filters.add(rankFilter);
            }
            
            
            if (condition.hasClickOrderRange()) {
                Filter clickFilter = createClickOrderFilter(condition.getMinClickOrder(), condition.getMaxClickOrder());
                filters.add(clickFilter);
            }
            
            
            if (!filters.isEmpty()) {
                FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
                scan.setFilter(filterList);
            }
            
            List<Result> results = new ArrayList<>();
            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    results.add(result);
                }
            }
            
            log.info("Search found {} records with condition: {}", results.size(), condition);
            return results;
        }
    }
    
    private List<Result> getResultsByRowKeys(List<String> rowKeys) throws IOException {
        List<Result> results = new ArrayList<>();
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            for (String rowKey : rowKeys) {
                Get get = new Get(Bytes.toBytes(rowKey));
                Result result = table.get(get);
                if (!result.isEmpty()) {
                    results.add(result);
                }
            }
        }
        return results;
    }
    
    private void setupTimeRangeScan(Scan scan, SearchCondition condition) {

        String reverseStart = condition.getReverseStartTimeStr(); 
        String reverseEnd = condition.getReverseEndTimeStr();

        log.debug("设置时间范围扫描: startTime={}, endTime={}, reverseStart={}, reverseEnd={}", 
                  condition.getStartTime(), condition.getEndTime(), reverseStart, reverseEnd);
        
        if (reverseStart.equals(reverseEnd)) {
            log.debug("开始时间等于结束时间，不设置扫描范围，仅依赖过滤器");
        } else {
            log.debug("扫描范围: startRow=00|{}, stopRow=16|{}", reverseStart, reverseEnd);
            scan.setStartRow(Bytes.toBytes("00|" + reverseStart));
            scan.setStopRow(Bytes.toBytes("16|" + reverseEnd));
        }
    }
    
    private Filter createTimeFilter(SearchCondition condition) {
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss");
        String startTimeStr = condition.getStartTime().format(formatter);
        String endTimeStr = condition.getEndTime().format(formatter);
        
        if (startTimeStr.equals(endTimeStr)) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                CF,
                COL_TS,
                CompareOperator.EQUAL,
                Bytes.toBytes(startTimeStr)
            );
            filter.setFilterIfMissing(true);
            log.debug("创建时间相等过滤器: ts = {}", startTimeStr);
            return filter;
        } else {
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            
            SingleColumnValueFilter startFilter = new SingleColumnValueFilter(
                CF,
                COL_TS,
                CompareOperator.GREATER_OR_EQUAL,
                Bytes.toBytes(startTimeStr)
            );
            startFilter.setFilterIfMissing(true);
            
            SingleColumnValueFilter endFilter = new SingleColumnValueFilter(
                CF,
                COL_TS,
                CompareOperator.LESS_OR_EQUAL,
                Bytes.toBytes(endTimeStr)
            );
            endFilter.setFilterIfMissing(true);
            
            filterList.addFilter(startFilter);
            filterList.addFilter(endFilter);
            
            log.debug("创建时间范围过滤器: {} <= ts <= {}", startTimeStr, endTimeStr);
            return filterList;
        }
    }
    
    private Filter createUserFilter(List<String> userIds) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (String userId : userIds) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                CF,
                COL_USER,
                CompareOperator.EQUAL,
                Bytes.toBytes(userId)
            );
            filter.setFilterIfMissing(true);
            filterList.addFilter(filter);
        }
        return filterList;
    }
    
    private Filter createQueryFilter(List<String> keywords) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (String keyword : keywords) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                CF,
                COL_QUERY,
                CompareOperator.EQUAL,
                new SubstringComparator(keyword)
            );
            filter.setFilterIfMissing(true);
            filterList.addFilter(filter);
        }
        return filterList;
    }
    
    private Filter createDomainFilter(List<String> keywords) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (String keyword : keywords) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                CF,
                COL_DOMAIN,
                CompareOperator.EQUAL,
                new SubstringComparator(keyword)
            );
            filter.setFilterIfMissing(true);
            filterList.addFilter(filter);
        }
        return filterList;
    }

    private Filter createUrlFilter(List<String> keywords) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (String keyword : keywords) {
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                CF,
                COL_URL,
                CompareOperator.EQUAL,
                new SubstringComparator(keyword)
            );
            filter.setFilterIfMissing(true);
            filterList.addFilter(filter);
        }
        return filterList;
    }

    private Filter createRankFilter(int minRank, int maxRank) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        
        
        SingleColumnValueFilter minFilter = new SingleColumnValueFilter(
            CF,
            COL_RANK,
            CompareOperator.GREATER_OR_EQUAL,
            Bytes.toBytes(minRank)
        );
        minFilter.setFilterIfMissing(true);
        filterList.addFilter(minFilter);
        
        
        SingleColumnValueFilter maxFilter = new SingleColumnValueFilter(
            CF,
            COL_RANK,
            CompareOperator.LESS_OR_EQUAL,
            Bytes.toBytes(maxRank)
        );
        maxFilter.setFilterIfMissing(true);
        filterList.addFilter(maxFilter);
        
        return filterList;
    }
    
    private Filter createClickOrderFilter(int minClickOrder, int maxClickOrder) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        
        
        SingleColumnValueFilter minFilter = new SingleColumnValueFilter(
            CF,
            COL_CLICK,
            CompareOperator.GREATER_OR_EQUAL,
            Bytes.toBytes(minClickOrder)
        );
        minFilter.setFilterIfMissing(true);
        filterList.addFilter(minFilter);
        
        
        SingleColumnValueFilter maxFilter = new SingleColumnValueFilter(
            CF,
            COL_CLICK,
            CompareOperator.LESS_OR_EQUAL,
            Bytes.toBytes(maxClickOrder)
        );
        maxFilter.setFilterIfMissing(true);
        filterList.addFilter(maxFilter);
        
        return filterList;
    }
    
    public Result getByRowKey(String rowKey) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get get = new Get(Bytes.toBytes(rowKey));
            return table.get(get);
        }
    }

    public static void printResult(Result result) {
        Map<String, Object> map = resultToMap(result);
        System.out.println("行键: " + map.get("rowKey"));
        System.out.println("  时间: " + map.get("accessTime"));
        System.out.println("  用户: " + map.get("userId"));
        System.out.println("  查询词: " + map.get("query"));
        System.out.println("  排名: " + map.get("rank"));
        System.out.println("  点击顺序: " + map.get("clickOrder"));
        System.out.println("  URL: " + map.get("url"));
        System.out.println("  域名: " + map.get("domain"));
        System.out.println();
    }
    
    public static Map<String, Object> resultToMap(Result result) {
        try {
            Map<String, Object> map = new java.util.HashMap<>();
            String rowKey = Bytes.toString(result.getRow());
            
            // 直接使用Bytes.toString处理，如果getValue返回null，Bytes.toString会处理
            String accessTime = Bytes.toString(result.getValue(CF, COL_TS));
            String userId = Bytes.toString(result.getValue(CF, COL_USER));
            String query = Bytes.toString(result.getValue(CF, COL_QUERY));
            String url = Bytes.toString(result.getValue(CF, COL_URL));
            String domain = Bytes.toString(result.getValue(CF, COL_DOMAIN));
            
            // 对于数值类型，需要特殊处理
            int rank = 0;
            int clickOrder = 0;
            try {
                byte[] rankBytes = result.getValue(CF, COL_RANK);
                if (rankBytes != null) {
                    rank = Bytes.toInt(rankBytes);
                }
            } catch (Exception e) {
                // 忽略转换错误
            }
            
            try {
                byte[] clickBytes = result.getValue(CF, COL_CLICK);
                if (clickBytes != null) {
                    clickOrder = Bytes.toInt(clickBytes);
                }
            } catch (Exception e) {
                // 忽略转换错误
            }
            
            map.put("rowKey", rowKey);
            map.put("accessTime", accessTime != null ? accessTime : "");
            map.put("userId", userId != null ? userId : "");
            map.put("query", query != null ? query : "");
            map.put("rank", rank);
            map.put("clickOrder", clickOrder);
            map.put("url", url != null ? url : "");
            map.put("domain", domain != null ? domain : "");
            return map;
        } catch (Exception e) {
            // 如果转换失败，返回空map但记录错误
            log.error("转换Result到Map时出错", e);
            Map<String, Object> map = new java.util.HashMap<>();
            map.put("error", "数据转换错误: " + e.getMessage());
            return map;
        }
    }
}