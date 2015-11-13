package org.cloudburst.etl.util;

/**
 * Class that represent the Q3 data. It implement comparable so it can be sorted.
 */
public class Q3Object implements Comparable {
    private String impactScore;
    private String tweetId;
    private String text;

    public Q3Object(String impactScore, String tweetId, String text) {
        this.impactScore = impactScore;
        this.tweetId = tweetId;
        this.text = text;
    }

    public String getImpactScore() {
        return impactScore;
    }

    public void setImpactScore(String impactScore) {
        this.impactScore = impactScore;
    }

    public String getTweetId() {
        return tweetId;
    }

    public void setTweetId(String tweetId) {
        this.tweetId = tweetId;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public int compareTo(Object o) {
        return -1 * this.impactScore.compareTo(((Q3Object) o).getImpactScore());
    }
}
