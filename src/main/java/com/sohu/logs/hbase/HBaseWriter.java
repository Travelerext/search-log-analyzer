package com.sohu.logs.hbase;

import com.sohu.logs.model.LogRecord;
import com.sohu.logs.util.TimeUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseWriter implements Closeable {

    private final BufferedMutator mutator;
    private static final byte[] CF = Bytes.toBytes("cf");
    private static final int SALT_BUCKETS = 16;

    public HBaseWriter(Connection conn) throws IOException {
        mutator = conn.getBufferedMutator(
                new BufferedMutatorParams(TableName.valueOf("search_logs"))
                        .writeBufferSize(8 * 1024 * 1024)
        );
    }

    private String salt(String key) {
        int h = Math.abs(key.hashCode() % SALT_BUCKETS);
        return String.format("%02d|", h);
    }

    private String buildRowKey(LogRecord r) {
        
        
        return salt(r.userId) 
                + TimeUtils.toReverseTimestampStr(r.ts)
                + "#" + r.userId 
                + "#" + r.domain;
    }

    public void writeBatch(List<LogRecord> batch) throws IOException {
        List<Mutation> puts = new ArrayList<>();

        for (LogRecord r : batch) {
            String rowKey = buildRowKey(r);

            Put p = new Put(Bytes.toBytes(rowKey));
            p.addColumn(CF, Bytes.toBytes("ts"), Bytes.toBytes(r.tsStr));
            p.addColumn(CF, Bytes.toBytes("user"), Bytes.toBytes(r.userId));
            p.addColumn(CF, Bytes.toBytes("query"), Bytes.toBytes(r.query));
            p.addColumn(CF, Bytes.toBytes("rank"), Bytes.toBytes(r.rank));
            p.addColumn(CF, Bytes.toBytes("click"), Bytes.toBytes(r.clickOrder));
            p.addColumn(CF, Bytes.toBytes("url"), Bytes.toBytes(r.url));
            p.addColumn(CF, Bytes.toBytes("domain"), Bytes.toBytes(r.domain));
            
            puts.add(p);
        }

        if (!puts.isEmpty()) {
            mutator.mutate(puts);
        }
    }

    public void flush() throws IOException {
        mutator.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
        mutator.close();
    }
}