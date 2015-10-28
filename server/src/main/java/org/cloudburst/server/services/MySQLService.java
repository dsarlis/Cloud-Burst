package org.cloudburst.server.services;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.cloudburst.server.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLService {

	private static final char SPACE = ' ';
	private static final char PLUS = '+';
	private static final String COLON = ":";
	private static final String NEW_LINE = "\n";
	private static final String SELECT_QUERY = "SELECT * FROM tweets WHERE userId=? AND creationTime=? ORDER BY tweetId";

	private final static Logger logger = LoggerFactory.getLogger(MySQLService.class);

	private MySQLConnectionFactory factory;

	public MySQLService(MySQLConnectionFactory factory) {
		this.factory = factory;
	}

	public String testStatement() {
		StringBuilder builder = new StringBuilder();

		try (Connection connection = factory.getConnection()) {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("select * from tweets");

			while (rs.next()) {
				builder.append(rs.getInt(1) + ",");
			}
		} catch (SQLException ex) {
			logger.error("Problem executing statement", ex);
		}

		return builder.toString();
	}

	public String getTweetResult(long userId, String creationTime) {

		StringBuilder builder = new StringBuilder();
		try (Connection connection = factory.getConnection()) {
			PreparedStatement preparedStatement = connection.prepareStatement(SELECT_QUERY);
			preparedStatement.setLong(1, userId);
			preparedStatement.setTimestamp(2, Timestamp.valueOf(creationTime.replace(PLUS, SPACE)));

			ResultSet rs = preparedStatement.executeQuery();

			while (rs.next()) {
				builder.append(rs.getLong("tweetId")).append(COLON);
				builder.append(rs.getInt("score")).append(COLON);
				builder.append(new String(rs.getBytes("text"), StandardCharsets.UTF_8).replace(";", "\n"));
				builder.append(NEW_LINE);
			}
		} catch (SQLException ex) {
			logger.error("Problem executing statement", ex);
		}

		return builder.toString();
	}

}
