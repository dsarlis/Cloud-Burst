package org.cloudburst.etl.model;

import java.util.Date;

public class Tweet {

    private long tweetId;
    private long usedId;
    private Date creationTime;
    private String text;
    private int score;

    public Tweet(long tweetId, long usedId, Date creationTime, String text, int score) {
        this.tweetId = tweetId;
        this.usedId = usedId;
        this.creationTime = creationTime;
        this.text = text;
        this.score = score;
    }

    public long getTweetId() {
        return tweetId;
    }

    public void setTweetId(long tweetId) {
        this.tweetId = tweetId;
    }

    public long getUsedId() {
        return usedId;
    }

    public void setUsedId(long usedId) {
        this.usedId = usedId;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Date creationTime) {
        this.creationTime = creationTime;
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

}
