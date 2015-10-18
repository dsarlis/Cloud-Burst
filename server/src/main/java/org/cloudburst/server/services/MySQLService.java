package org.cloudburst.server.services;


import org.cloudburst.server.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLService {

    private final static Logger logger = LoggerFactory.getLogger(MySQLService.class);

    private MySQLConnectionFactory factory;

    public MySQLService(MySQLConnectionFactory factory) {
        this.factory = factory;
    }

    public String testStatement() {
        StringBuilder builder = new StringBuilder();

        try (Connection connection = factory.getConnection()){
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select * from t1");

            while(rs.next()){
                builder.append(rs.getInt(1) + ",");
            }
        } catch (SQLException ex) {
            logger.error("Problem executing statement", ex);
        }

        return builder.toString();
    }

}
