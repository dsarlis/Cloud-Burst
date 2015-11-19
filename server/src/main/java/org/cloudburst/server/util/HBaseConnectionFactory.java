package org.cloudburst.server.util;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

/**
 * This class takes care of the HBaseConnections
 */
public class HBaseConnectionFactory {
    private static HTable connection = null;

    public static void init(Properties hbaseConfigProperties) {
        /* Configuring HBase */
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hbaseConfigProperties.getProperty("zookeeper.quorum"));
        config.set("hbase.zookeeper.property.clientPort", hbaseConfigProperties.getProperty("zookeeper.port"));
        config.set("hbase.master", hbaseConfigProperties.getProperty("master"));

        try {
            connection = new HTable(config, "hashtags");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets an HBase connection.
     */
    public HTable getConnection() {
        return connection;
    }

    /**
     * Shuts down the connection.
     */
    public static void shutDown() throws IOException {
        connection.close();
    }
}
