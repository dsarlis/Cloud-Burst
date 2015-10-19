package org.cloudburst.server.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseService {
    private final static Logger logger = LoggerFactory.getLogger(HBaseService.class);

    private HTable table;
    private String tableName;
    private String columnFamily;
    private String qualifier;

    public HBaseService(String tableName, String columnFamily, String qualifier) {
        Configuration config = HBaseConfiguration.create();
        try {
            this.table = new HTable(config, tableName);
        } catch (IOException e) {
            logger.error("IO Exception while trying to connect to HBase table", e);
        }
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.qualifier = qualifier;
    }

    public String getRecord(String rowKey) {
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = null;
        try {
            result = table.get(get);
        } catch (IOException e) {
            logger.error("IO Exception when trying to read from HBase table", e);
        }
        byte[] value = result.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier));

        return Bytes.toString(value);
    }
}
