package org.cloudburst.server.services;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.cloudburst.server.util.HBaseConnectionFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class HBaseService {
    private HBaseConnectionFactory factory;
    private static final String COLON = ":";

    public HBaseService(HBaseConnectionFactory factory) {
        this.factory = factory;
    }

    public String getQ2Record(String rowKey, String tableName, String columnFamily, String qualifier) {
        HTableInterface table = null;
            table = factory.getConnection();
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = null;
            try {
                result = table.get(get);
            } catch (IOException e) {
                System.err.println("IO Exception when trying to read from HBase table");
            }
            byte[] value = result.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes(qualifier));

            return Bytes.toString(value);
    }

    public String getQ4Record(String rowKey, String tableName, String columnFamily, long topN) {
        HTable table = factory.getConnection();
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = null;
        try {
            result = table.get(get);
        } catch (IOException e) {
            System.err.println("IO Exception when trying to read from HBase table");
        }
        StringBuilder resultValue = new StringBuilder();
        for (int i = 0; i < topN; i++) {
            byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(i));
            String[] parts = Bytes.toString(value).split(COLON);
            try {
                String decodedText = new String(Hex.decodeHex(parts[3].toCharArray()), "UTF-8");
                resultValue.append(parts[0]).append(COLON);
                resultValue.append(parts[1]).append(COLON);
                resultValue.append(parts[2]).append(COLON);
                resultValue.append(decodedText).append("\n");
            } catch (DecoderException e) {
                e.printStackTrace();
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }
        return resultValue.toString();
    }
}
