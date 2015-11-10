package org.cloudburst.server.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;

import java.io.IOException;
import java.util.Properties;

public class HBaseConnectionFactory {
    private static HConnection connection = null;

    public static void init(Properties hbaseConfigProperties) {
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", hbaseConfigProperties.getProperty("zookeeper.quorum"));
        config.set("hbase.zookeeper.property.clientPort", hbaseConfigProperties.getProperty("zookeeper.port"));
        config.set("hbase.master", hbaseConfigProperties.getProperty("master"));

        try {
            connection = HConnectionManager.createConnection(config);
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
    }

    public HConnection getConnection() {
        return connection;
    }

    public static void shutDown() throws IOException {
        connection.close();
    }
}
