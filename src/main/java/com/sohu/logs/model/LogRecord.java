package com.sohu.logs.model;

import java.util.List;

public class LogRecord {
    public long ts;                 
    public String tsStr;           
    public String userId;          
    public String query;           
    public int rank;               
    public int clickOrder;         
    public String url;             
    public String domain;          
    public List<String> tokens;    
    
    
    public LogRecord() {}
    
    @Override
    public String toString() {
        return "LogRecord{" +
                "ts=" + ts +
                ", tsStr='" + tsStr + '\'' +
                ", userId='" + userId + '\'' +
                ", query='" + query + '\'' +
                ", rank=" + rank +
                ", clickOrder=" + clickOrder +
                ", url='" + url + '\'' +
                ", domain='" + domain + '\'' +
                '}';
    }
}