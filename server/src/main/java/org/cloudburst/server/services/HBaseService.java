package org.cloudburst.server.services;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudburst.server.util.HBaseConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HBaseService {
    private final static Logger logger = LoggerFactory.getLogger(HBaseService.class);
    private HBaseConnectionFactory factory;

    public HBaseService(HBaseConnectionFactory factory) {
        this.factory = factory;
    }

    public String getQ2Record(String rowKey, String tableName, String columnFamily, String qualifier) {
        HTableInterface table = null;
        try {
            table = factory.getConnection().getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = null;
            try {
                result = table.get(get);
            } catch (IOException e) {
                logger.error("IO Exception when trying to read from HBase table", e);
            }
            byte[] value = result.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier));

            return Bytes.toString(value);
        } catch (IOException e) {
            return null;
        }
    }

    public String getQ4Record(String rowKey, String tableName, String columnFamily, long topN) {
        HTableInterface table = null;
        try {
            table = factory.getConnection().getTable(tableName);
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = null;
            try {
                result = table.get(get);
            } catch (IOException e) {
                logger.error("IO Exception when trying to read from HBase table", e);
            }
            StringBuilder resultValue = new StringBuilder();
            for (int i = 0; i < topN; i++) {
                byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(""+i));
                resultValue.append(Bytes.toString(value)).append("\n");
            }

            return resultValue.toString();
        } catch (IOException e) {
            return null;
        }
    }
}
