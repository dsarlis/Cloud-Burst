package org.cloudburst.server.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
import java.util.Properties;

public class HBaseConnectionFactory {
    private static HTable connection = null;

    public static void init(Properties hbaseConfigProperties) {
        Configuration config = HBaseConfiguration.create();
        System.out.println("I created config object");
        config.set("hbase.zookeeper.quorum", hbaseConfigProperties.getProperty("zookeeper.quorum"));
        config.set("hbase.zookeeper.property.clientPort", hbaseConfigProperties.getProperty("zookeeper.port"));
        config.set("hbase.master", hbaseConfigProperties.getProperty("master"));

        try {
            connection = new HTable(config, "hashtags");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public HTable getConnection() {
        return connection;
    }

    public static void shutDown() throws IOException {
        connection.close();
    }
}
