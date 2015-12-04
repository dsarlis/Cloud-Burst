package org.cloudburst.etl.util;

/**
 * Class that represent the Q3 data. It implement comparable so it can be
 * sorted.
 */
public class Q3Object implements Comparable<Q3Object> {
    private long impactScore;
    private long tweetId;
    private String text;

    public Q3Object(String impactScore, String tweetId, String text) {
        this.impactScore = Long.parseLong(impactScore);
        this.tweetId = Long.parseLong(tweetId);
        this.text = text;
    }

    public long getImpactScore() {
        return impactScore;
    }

    public long getTweetId() {
        return tweetId;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    /* Sort according to impactScore in descending order */
    @Override
    public int compareTo(Q3Object other) {
        long impactScoreAbs = Math.abs(impactScore);
        long otherImpactScoreAbs = Math.abs(other.getImpactScore());

        if (impactScoreAbs == otherImpactScoreAbs) {
            return tweetId == other.getTweetId() ? 0 : (tweetId < other.getTweetId() ? -1 : 1);

        }
        return impactScoreAbs < otherImpactScoreAbs ? 1 : -1;
    }
}
