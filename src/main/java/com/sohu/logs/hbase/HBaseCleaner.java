package com.sohu.logs.hbase;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public class HBaseCleaner {
    private static final String TABLE_NAME = "search_logs";

    public static void clearAllData(Connection connection) throws Exception {
        try (Admin admin = connection.getAdmin()) {
            TableName tn = TableName.valueOf(TABLE_NAME);
            if (!admin.tableExists(tn)) {
                System.out.println("表 " + TABLE_NAME + " 不存在，无需清空");
                return;
            }

            System.out.println("正在清空表 " + TABLE_NAME + "...");
            
            try {
                
                if (admin.isTableEnabled(tn)) {
                    System.out.println("禁用表 " + TABLE_NAME + "...");
                    admin.disableTable(tn);
                }
                
                
                System.out.println("删除表 " + TABLE_NAME + "...");
                admin.deleteTable(tn);
                
                
                System.out.println("重新创建表 " + TABLE_NAME + "...");
                HBaseSchemaCreator.createTableIfNotExists(connection, TABLE_NAME);
                
                System.out.println("表 " + TABLE_NAME + " 已清空并重新创建");
            } catch (Exception e) {
                throw new Exception("清空表失败: " + e.getMessage(), e);
            }
        }
    }
}