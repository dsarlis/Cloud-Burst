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

    public static void main(String[] args) {
        try {
            System.out.println(new String(Hex.decodeHex("525420406d696b6f736869626f7965733a2023e38193e381aee7bf94e3818fe38293e381abe6839ae3828ce79bb4e38197e3819fe4baba52540a0ae38186e381a1726f6c65e381a8e3818be79fa5e38289e381aae3818be381a3e3819fe38293e381a7e38199e38191e381a9e280a60ae381a6e3818be58db3e88888e381a7e38193e38293e381aae38193e381a8e8a880e38188e381a1e38283e38186e381aee381ade280a620687474703a2f2f742e636f2f51714e3332456d55656d".toCharArray())));
            System.out.println("知らなかったんですけど…");
        }
        catch (DecoderException e) {
            e.printStackTrace();
        }
    }
}
