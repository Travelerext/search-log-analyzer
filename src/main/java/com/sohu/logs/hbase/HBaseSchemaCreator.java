package com.sohu.logs.hbase;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSchemaCreator {
    private static byte[][] getSplits(int regions) {
        byte[][] splits = new byte[regions - 1][];
        for (int i = 1; i < regions; i++) {
            splits[i - 1] = Bytes.toBytes(String.format("%02d|", i));
        }
        return splits;
    }
    
    public static void createTableIfNotExists(Admin admin, String tableName, String[] families, int regions) throws Exception {
        TableName tn = TableName.valueOf(tableName);
        if (!admin.tableExists(tn)) {
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(tn);
            for (String cf : families) {
                ColumnFamilyDescriptor cfd = ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes(cf))
                        .setBlockCacheEnabled(true)
                        .build();
                tdb.setColumnFamily(cfd);
            }

            admin.createTable(tdb.build(), getSplits(regions));
            System.out.println("Created " + tableName + " with " + regions + " regions.");
        } else {
            System.out.println("Table " + tableName + " already exists.");
        }
    }
    
    public static void createTableIfNotExists(Admin admin, String tableName) throws Exception {
        createTableIfNotExists(admin, tableName, new String[]{"cf"}, 16);
    }
    
    public static void createTableIfNotExists(Connection connection, String tableName) throws Exception {
        try (Admin admin = connection.getAdmin()) {
            createTableIfNotExists(admin, tableName);
        }
    }
}