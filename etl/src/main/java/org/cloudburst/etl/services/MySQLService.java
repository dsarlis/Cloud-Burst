package org.cloudburst.etl.services;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.List;

import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLService {

	private static final Logger logger = LoggerFactory.getLogger(MySQLService.class);
	private static final String INSERT_QUERY = "insert delayed into tweets (tweetId, userId, creationTime, text, score) values ";
	private static final int COLUMN_COUNT = 5;

	private MySQLConnectionFactory factory;

	public MySQLService(MySQLConnectionFactory factory) throws SQLException {
		this.factory  = factory;
	}

	private String getInsertPlaceholders(int placeholderCount) {
		StringBuilder builder = new StringBuilder("(");

		for ( int i = 0; i < placeholderCount; i++ ) {
			if ( i != 0 ) {
				builder.append(",");
			}
			builder.append("?");
		}
		return builder.append(")").toString();
	}

	public void insertTweets(List<Tweet> tweets) throws ParseException {
		if (tweets.size() <= 0) {
			return;
		}
		try(Connection connection = factory.getConnection()) {
			StringBuilder builder = new StringBuilder(INSERT_QUERY);
			String placeholders = getInsertPlaceholders(COLUMN_COUNT);

			for ( int i = 0; i < tweets.size(); i++ ) {
				if ( i != 0 ) {
					builder.append(",");
				}
				builder.append(placeholders);
			}

			String query = builder.toString();
			PreparedStatement preparedStatement = connection.prepareStatement(query);
			int counter = 1;

			for (Tweet tweet : tweets) {
				preparedStatement.setLong(counter++, tweet.getTweetId());
				preparedStatement.setLong(counter++, tweet.getUserId());
				preparedStatement.setTimestamp(counter++, new Timestamp(tweet.getCreationTime().getTime()));
				preparedStatement.setBytes(counter++, tweet.getText().getBytes());
				preparedStatement.setInt(counter++, tweet.getScore());
			}
			preparedStatement.execute();
		} catch (SQLException ex) {
			logger.error("Problem executing sql query", ex);
		}
	}

}