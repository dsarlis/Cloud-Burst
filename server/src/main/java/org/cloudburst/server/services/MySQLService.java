package org.cloudburst.server.services;

import static org.cloudburst.server.util.TextCensor.censorBannedWords;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import org.cloudburst.server.util.MySQLConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This service interacts with MySQL server.
 */
public class MySQLService {

    /* String constants */
    private static final String NEW_LINE = "\n";
    private static final char SPACE = ' ';
    private static final char PLUS = '+';
    private static final String COLON = ":";
    private static final String COMMA = ",";

    private static final String NEGATIVE_HEADER = NEW_LINE + "Negative Tweets" + NEW_LINE;
    private static final String POSITIVE_HEADER = "Positive Tweets" + NEW_LINE;

    /* Queries for all questions. */
    private static final String Q2 = "SELECT * FROM tweets WHERE userId=? AND creationTime=? ORDER BY tweetId";
    private static final String Q3 = "(SELECT * FROM q3 WHERE userId=@userId AND creationTime BETWEEN '@start' AND '@end' AND impactScore > 0 ORDER BY impactScore DESC, tweetId ASC LIMIT @limit) UNION ALL (SELECT * FROM q3 WHERE userId=@userId AND creationTime BETWEEN '@start' AND '@end' AND impactScore < 0 ORDER BY impactScore ASC, tweetId ASC LIMIT @limit);";
    private static final String Q4 = "SELECT * FROM hashtags WHERE hashtag=? ORDER BY totalHashTagCount DESC, createdAtDate ASC LIMIT ?;";

    private final static Logger logger = LoggerFactory.getLogger(MySQLService.class);

    private MySQLConnectionFactory factory;

    public MySQLService(MySQLConnectionFactory factory) {
        this.factory = factory;
    }

    /**
     * Return formatted result with the (tweet id, score) from a given user and
     * time-stamp.
     */
    public String getTweetResult(long userId, String creationTime) {

        StringBuilder builder = new StringBuilder();
        try (Connection connection = factory.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(Q2);
            preparedStatement.setLong(1, userId);
            preparedStatement.setTimestamp(2, Timestamp.valueOf(creationTime.replace(PLUS, SPACE)));

            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                builder.append(rs.getLong("tweetId")).append(COLON);
                builder.append(rs.getInt("score")).append(COLON);
                builder.append(
                        censorBannedWords(new String(rs.getBytes("text"), StandardCharsets.UTF_8)).replace(";", "\n"));
                builder.append(NEW_LINE);
            }
        } catch (SQLException ex) {
            logger.error("Problem executing statement", ex);
        }

        return builder.toString();
    }

    /**
     * Return the top N tweets (each for Positive/Negative impact scores. )
     */
    public String getTweetImpactScore(String start, String end, String userId, String limit) {

        StringBuilder positiveBuilder = new StringBuilder(POSITIVE_HEADER);
        StringBuilder negativeBuilder = new StringBuilder(NEGATIVE_HEADER);

        try (Connection connection = factory.getConnection()) {

            final String query = Q3.replace("@userId", userId).replace("@start", start).replace("@end", end)
                    .replace("@limit", limit);
            ResultSet result = connection.prepareStatement(query).executeQuery();

            /*
             * Sends to appropriate StringBuilder depending over the
             * impactScore.
             */
            while (result.next()) {
                long impactScore = result.getLong("impactScore");
                if (impactScore > 0) {
                    buildResponseForTweetImpact(result, positiveBuilder, impactScore);
                } else if (impactScore < 0) {
                    buildResponseForTweetImpact(result, negativeBuilder, impactScore);
                }
            }
        } catch (SQLException ex) {
            logger.error("Problem executing statement", ex);
        }

        return positiveBuilder.append(negativeBuilder).toString();
    }

    /**
     * Builds response as per the impact score (Positive/Negative)
     */
    private void buildResponseForTweetImpact(ResultSet rs, StringBuilder builder, long impactScore)
            throws SQLException {
        builder.append(rs.getDate("creationTime")).append(COMMA);
        builder.append(impactScore).append(COMMA);
        builder.append(rs.getLong("tweetId")).append(COMMA);
        builder.append(censorBannedWords(new String(rs.getBytes("text"), StandardCharsets.UTF_8)).replace(";", "\n"));
        builder.append(NEW_LINE);
    }

    /**
     * Returns the top hashtag sorted by hashtagCount and CreatedDate.
     */
    public String getTopHashtags(String hashtag, int limit) {

        StringBuilder builder = new StringBuilder();
        try (Connection connection = factory.getConnection()) {
            PreparedStatement preparedStatement = connection.prepareStatement(Q4);
            preparedStatement.setBytes(1, hashtag.getBytes());
            preparedStatement.setInt(2, limit);

            ResultSet rs = preparedStatement.executeQuery();

            while (rs.next()) {
                builder.append(rs.getDate("createdAtDate")).append(COLON);
                builder.append(rs.getInt("totalHashTagCount")).append(COLON);
                builder.append(rs.getString("sortedUniqueUserList")).append(COLON);
                builder.append(new String(rs.getBytes("originTweetText"), StandardCharsets.UTF_8).replace(";", "\n"));
                builder.append(NEW_LINE);
            }
        } catch (SQLException ex) {
            logger.error("Problem executing statement", ex);
        }

        return builder.toString();
    }

}
