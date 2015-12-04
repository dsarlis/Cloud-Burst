package org.cloudburst.etl.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.TimeZone;

import org.cloudburst.etl.model.Tweet;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class TweetUtil {
    public static final String TIME_ZONE_UTC_GMT = "+0000";

    private static final String OLD_TWEETS_DATE_STR = "Sat Apr 19 00:00:00 +0000 2014";
    private static final String DATE_TIME_FORMAT = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";

    private static Date oldTweetsDate;

    public static Date toUTCDate(String dateStr) throws ParseException {
        SimpleDateFormat format = new SimpleDateFormat(DATE_TIME_FORMAT, Locale.ENGLISH);

        format.setTimeZone(TimeZone.getTimeZone(TIME_ZONE_UTC_GMT));
        return format.parse(dateStr);
    }

    /**
     * Read json tweet.
     */
    public static Tweet generateTweet(JsonElement jsonElement) {
        try {
            JsonObject tweetObject = jsonElement.getAsJsonObject();
            JsonObject userObject = tweetObject.getAsJsonObject("user");
            java.util.Map<String, Integer> hashTags = new HashMap<String, Integer>();

            if (tweetObject.has("entities")) {
                JsonObject entitiesObject = tweetObject.get("entities").getAsJsonObject();

                if (entitiesObject.has("hashtags")) {
                    JsonArray hashTagsArray = entitiesObject.get("hashtags").getAsJsonArray();

                    for (JsonElement hashTag : hashTagsArray) {
                        String text = hashTag.getAsJsonObject().get("text").getAsString();
                        Integer count = hashTags.get(text);

                        hashTags.put(text, count != null ? count + 1 : 1);
                    }
                }
            }

            return new Tweet(tweetObject.get("id").getAsLong(), userObject.get("id").getAsLong(),
                    userObject.get("followers_count").getAsInt(), tweetObject.get("created_at").getAsString(),
                    tweetObject.get("text").getAsString(), hashTags);
        } catch (Throwable ex) {
            return null;
        }
    }

    /**
     * Returns old tweets maximum date
     */
    public static Date getOldTweetsDate() {
        if (oldTweetsDate == null) {
            try {
                oldTweetsDate = toUTCDate(OLD_TWEETS_DATE_STR);
            } catch (ParseException e) {
            }
        }
        return oldTweetsDate;
    }

    /**
     * Ignore all tweets that have a time stamp prior to Sun, 20 Apr 2014
     */
    public static boolean isTweetOld(Tweet tweet) throws ParseException {
        return tweet.getCreationTime().before(getOldTweetsDate());
    }

    /**
     * Checks for Malformed Tweets.
     */
    public static JsonElement throwExceptionForMalformedTweets(String line) throws JsonSyntaxException {
        return new JsonParser().parse(line);
    }
}
