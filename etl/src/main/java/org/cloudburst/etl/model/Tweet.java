package org.cloudburst.etl.model;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.cloudburst.etl.util.StringUtil;
import org.cloudburst.etl.util.TweetUtil;

/**
 * GSON Mapping for Tweet.
 */
public class Tweet {

    private long tweetId;

    private int followersCount;

    private User user;

    private String createdAt;

    private String text;

    /* Not part of raw JSON */
    private Map<String, Integer> hashTags;

    private int sentimentScore;

    private Date createdAtDate = null;

    public Tweet(long tweetId, long userId, int followersCount, String createdAt, String text,
            Map<String, Integer> hashTags) throws ParseException {
        this.tweetId = tweetId;
        this.createdAt = createdAt;
        this.text = text;
        this.followersCount = followersCount;
        this.hashTags = hashTags;
        setUserId(userId);
    }

    public long getTweetId() {
        return tweetId;
    }

    public void setUserId(long userId) {
        this.user = new User();
        this.user.setUserId(userId);
    }

    public Date getCreationTime() throws ParseException {
        if (createdAtDate == null) {
            createdAtDate = TweetUtil.toUTCDate(createdAt);
        }
        return createdAtDate;
    }

    public User getUser() {
        return user;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }

    public int getFollowersCount() {
        return followersCount;
    }

    public void setFollowersCount(int followersCount) {
        this.followersCount = followersCount;
    }

    public void adjustScore(int sentimentScore) {
        this.sentimentScore += sentimentScore;
    }

    public int getImpactScore() {
        return sentimentScore * (1 + followersCount);
    }

    public Map<String, Integer> getHashTags() {
        return hashTags;
    }

    public void setHashTags(Map<String, Integer> hashTags) {
        this.hashTags = hashTags;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String separator = "\t";
        SimpleDateFormat timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        timeStampFormat.setTimeZone(TimeZone.getTimeZone(TweetUtil.TIME_ZONE_UTC_GMT));
        builder.append(separator);
        builder.append(user.getUserId());
        builder.append(separator);
        builder.append(timeStampFormat.format(createdAtDate));
        builder.append(separator);
        builder.append(sentimentScore);
        builder.append(separator);
        builder.append(StringUtil.bytesToHex(text.getBytes()));
        return builder.toString();
    }

}
