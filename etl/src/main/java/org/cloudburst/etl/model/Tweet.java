package org.cloudburst.etl.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * GSON Mapping for Tweet.
 */
public class Tweet {

	private static final String DATE_TIME_FORMAT = "EEE MMM dd HH:mm:ss Z yyyy";
	private static final String TIME_ZONE_UTC_GMT = "GMT";

	private long tweetId;

	private User user;

	private String createdAt;

	private String text;

	private Date createdAtDate = null;

	/* Not part of raw JSON */
	private int score;

	public Tweet(long tweetId, long userId, String createdAt, String text) throws ParseException {
		setTweetId(tweetId);
		setUserId(userId);
		setCreationTime(createdAt);
		setText(text);
	}

	public long getTweetId() {
		return tweetId;
	}

	public void setTweetId(long tweetId) {
		this.tweetId = tweetId;
	}

	public long getUserId() {
		return user.getUserId();
	}

	public void setUserId(long userId) {
		this.user = new User();
		this.user.setUserId(userId);
	}

	public Date getCreationTime() throws ParseException {
		if (createdAtDate == null) {
			createdAtDate = toUTCDate(createdAt);
		}
		return createdAtDate;
	}

	public void setCreationTime(String createdAt) throws ParseException {
		this.createdAt = createdAt;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public int getScore() {
		return score;
	}

	public void setScore(int score) {
		this.score = score;
	}

	public void adjustScore(int sentimentScore) {
		this.score += sentimentScore;
	}

	private Date toUTCDate(String createdAt) throws ParseException {
		SimpleDateFormat format = new SimpleDateFormat(DATE_TIME_FORMAT);

		format.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_UTC_GMT));
		return format.parse(createdAt);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		String separator = "\t";

		builder.append(tweetId);
		builder.append(separator);
		builder.append(user.getUserId());
		builder.append(separator);
		builder.append(createdAt);
		builder.append(separator);
		builder.append(score);
		builder.append(separator);
		builder.append(text.replace("\n", "\\n").replace("\r", "\\r"));
		builder.append("\n");
		return builder.toString();
	}

}
