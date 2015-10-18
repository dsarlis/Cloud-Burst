package org.cloudburst.server.util;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class MySQLConnectionFactory {

    private final static Logger logger = LoggerFactory.getLogger(MySQLConnectionFactory.class);
    private static BoneCP connectionPool = null;

    public static void init(Properties boneCPConfigProperties) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception ex) {
            logger.error("Problems loading MySQL driver", ex);
            return;
        }

        try {
            BoneCPConfig config = new BoneCPConfig();

            config.setUser(boneCPConfigProperties.getProperty("username"));
            config.setPassword(boneCPConfigProperties.getProperty("password"));
            config.setJdbcUrl(boneCPConfigProperties.getProperty("jdbcUrl"));
            config.setMinConnectionsPerPartition(Integer.parseInt(boneCPConfigProperties.getProperty("minConnectionsPerPartition")));
            config.setMaxConnectionsPerPartition(Integer.parseInt(boneCPConfigProperties.getProperty("maxConnectionsPerPartition")));
            config.setPartitionCount(Integer.parseInt(boneCPConfigProperties.getProperty("partitionCount")));
            config.setAcquireIncrement(Integer.parseInt(boneCPConfigProperties.getProperty("acquireIncrement")));
            connectionPool = new BoneCP(config);
        } catch (SQLException ex) {
            logger.error("Problems creating connection pool", ex);
        }
    }

    public Connection getConnection() throws SQLException {
        return connectionPool.getConnection();
    }

    public static void shutdown() {
        connectionPool.shutdown();
    }

}
