package org.cloudburst.etl.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.gson.annotations.SerializedName;

/**
 * GSON Mapping for Tweet.
 */
public class Tweet {

	private static final String DATE_TIME_FORMAT = "EEE MMM dd HH:mm:ss Z yyyy";
	private static final String TIME_ZONE_UTC_GMT = "GMT";

	@SerializedName("id")
	private long tweetId;

	@SerializedName("user")
	private User user;

	@SerializedName("created_at")
	private String createdAt;

	@SerializedName("text")
	private String text;

	/* Not part of raw JSON */
	private int score;

	public Tweet(long tweetId, long usedId, String createdAt, String text) throws ParseException {
		setTweetId(tweetId);
		setUserId(usedId);
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

	public void setUserId(long usedId) {
		this.user.setUserId(usedId);
	}

	public Date getCreationTime() throws ParseException {
		return toUTCDate(createdAt);
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

		builder.append("Tweet [tweetId=");
		builder.append(tweetId);
		builder.append(", userId=");
		builder.append(user.getUserId());
		builder.append(", createdAt=");
		builder.append(createdAt);
		builder.append(", text=");
		builder.append(text);
		builder.append(", score=");
		builder.append(score);
		builder.append("]");
		return builder.toString();
	}

}
