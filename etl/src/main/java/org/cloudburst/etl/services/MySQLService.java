package org.cloudburst.etl.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.concurrent.atomic.AtomicLong;

import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLService {

	private static final Logger logger = LoggerFactory.getLogger(MySQLService.class);
	private static final String INSERT_QUERY = "insert into tweet (tweetId, usedId, creationTime, text, score) values (?, ?, ?, ?, ?)";
	private static final int BATCH_SIZE = 100;

	private AtomicLong counter;
	private Connection connection;
	private PreparedStatement preparedStatement;

	private MySQLConnectionFactory factory;

	public MySQLService(MySQLConnectionFactory factory) throws SQLException {
		this.factory = factory;
		this.connection = factory.getConnection();
		this.counter = new AtomicLong(0);
		this.preparedStatement = connection.prepareStatement(INSERT_QUERY);
	}

	public void insertTweet(Tweet tweet) throws ParseException {
		logger.info("Inserting tweet={}", tweet);
		try {
			if (counter.getAndIncrement() == BATCH_SIZE) {
				counter.set(0);
				preparedStatement.executeBatch();
				preparedStatement = connection.prepareStatement(INSERT_QUERY);
			}

			preparedStatement.setLong(1, tweet.getTweetId());
			preparedStatement.setLong(2, tweet.getUserId());
			preparedStatement.setDate(3, new java.sql.Date(tweet.getCreationTime().getTime()));
			preparedStatement.setString(4, tweet.getText());
			preparedStatement.setInt(5, tweet.getScore());

			preparedStatement.addBatch();
		} catch (SQLException ex) {
			logger.error("Problem executing sql query", ex);
		}
		logger.info("Done inserting tweet={}", tweet);
	}

	public void flush() {
		try {
			counter.set(0);
			preparedStatement.executeBatch();
			preparedStatement = connection.prepareStatement(INSERT_QUERY);
		} catch (SQLException ex) {
			logger.error("Problem executing sql query", ex);
		}
	}

	public void close(){
		try {
			connection.close();
		} catch (SQLException ex) {
			logger.error("Problem closing connection", ex);
		}
	}

}