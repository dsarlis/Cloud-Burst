package org.cloudburst.etl.services;

import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLService {

    private static final Logger logger = LoggerFactory.getLogger(MySQLService.class);
    private static final String INSERT_QUERY = " insert into tweets (tweetId, usedId, creationTime, text, score) values (?, ?, ?, ?, ?)";

    private MySQLConnectionFactory factory;

    public MySQLService(MySQLConnectionFactory factory) {
        this.factory = factory;
    }

    public void insertTewet(Tweet tweet) {
        StringBuilder builder = new StringBuilder();

        try (Connection connection = factory.getConnection()) {
            PreparedStatement preparedStmt = connection.prepareStatement(INSERT_QUERY);

            preparedStmt.setLong(1, tweet.getTweetId());
            preparedStmt.setLong(2, tweet.getUsedId());
            preparedStmt.setDate(3, new java.sql.Date(tweet.getCreationTime().getTime()));
            preparedStmt.setString(4, tweet.getText());
            preparedStmt.setInt(5, tweet.getScore());
        } catch (SQLException ex) {
            logger.error("Problem inserting tweet", ex);
        }
    }

}
