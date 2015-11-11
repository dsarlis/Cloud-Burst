package org.cloudburst.server.services;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.cloudburst.server.util.MySQLConnectionFactory;
import org.cloudburst.server.util.TextCensor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to call MySQL.
 */
public class MySQLService {

    private static final char SPACE = ' ';
    private static final char PLUS = '+';
    private static final String COLON = ":";
    private static final String NEW_LINE = "\n";
    private static final String QUERY_2 = "SELECT * FROM tweets WHERE userId=? AND creationTime=? ORDER BY tweetId";
    private static final String QUERY_4 = "select * from hashtags where hashtag=? order by totalHashTagCount desc, createdAtDate asc limit ?;";

    private final static Logger logger = LoggerFactory.getLogger(MySQLService.class);

    private MySQLConnectionFactory factory;

    public MySQLService(MySQLConnectionFactory factory) {
        this.factory = factory;
    }

    /**
     * Return formatted result with the (tweet id, score) from a given user and
     * timestamp.
     */
    public String getTweetResult(long userId, String creationTime) {

        StringBuilder builder = new StringBuilder();
        try (Connection connection = factory.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(QUERY_2);
            preparedStatement.setLong(1, userId);
            preparedStatement.setTimestamp(2, Timestamp.valueOf(creationTime.replace(PLUS, SPACE)));

            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                builder.append(rs.getLong("tweetId")).append(COLON);
                builder.append(rs.getInt("score")).append(COLON);
                builder.append(TextCensor.censorBannedWords(new String(rs.getBytes("text"), StandardCharsets.UTF_8))
                        .replace(";", "\n"));
                builder.append(NEW_LINE);
            }
        } catch (SQLException ex) {
            logger.error("Problem executing statement", ex);
        }

        return builder.toString();
    }

    /**
     * Returns the top hashtag sorted by hashtagCount and CreatedDate.
     */
    public String getTopHashtags(String hashtag, int limit) {

        StringBuilder builder = new StringBuilder();
        try (Connection connection = factory.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(QUERY_4);
            preparedStatement.setBytes(1, hashtag.getBytes());
            preparedStatement.setInt(2, limit);

            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                /* Date:Count:[List of user ids]:SourceTweetText\n */
                builder.append(rs.getDate("createdAtDate")).append(COLON);
                builder.append(rs.getInt("totalHashTagCount")).append(COLON);
                builder.append(String.format("[%s]", rs.getString("sortedUniqueUserList"))).append(COLON);
                builder.append(
                        TextCensor.censorBannedWords(new String(rs.getBytes("originTweetText"), StandardCharsets.UTF_8))
                                .replace(";", "\n"));
                builder.append(NEW_LINE);
            }
        } catch (SQLException ex) {
            logger.error("Problem executing statement", ex);
        }

        return builder.toString();
    }
}
