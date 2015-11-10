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
    private final static String TABLE = "tweets";
    private final static String COLUMN_FAMILY = "tweetInfo";
    private final static String QUALIFIER = "data";
    private HBaseConnectionFactory factory;

    public HBaseService(HBaseConnectionFactory factory) {
        this.factory = factory;
    }

    public String getRecord(String rowKey) {
        HTableInterface table = null;
        try {
            table = factory.getConnection().getTable(Bytes.toBytes(TABLE));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = null;
            try {
                result = table.get(get);
            } catch (IOException e) {
                logger.error("IO Exception when trying to read from HBase table", e);
            }
            byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY),Bytes.toBytes(QUALIFIER));

            return Bytes.toString(value);
        } catch (IOException e) {
            return null;
        }
    }
}
