package org.cloudburst.etl;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.util.Q3Object;
import org.cloudburst.etl.util.TextCensor;
import org.cloudburst.etl.util.TextSentimentGrader;
import org.cloudburst.etl.util.TweetUtil;

import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;

/**
 * Main class to process Q3 files.
 */
public class Q3 {
    private static final String TAB = "\t";
    private static final String UNDERSCORE = "_";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        /**
         * Mapper method that keeps userId and date as the KEY, and all other
         * required fields as the VALUE.
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                JsonElement jsonElement = TweetUtil.throwExceptionForMalformedTweets(line);
                Tweet tweet = TweetUtil.generateTweet(jsonElement);

                if (tweet != null) {
                    TextSentimentGrader.addSentimentScore(tweet);
                    if (tweet.getImpactScore() != 0) {
                        Calendar calendar = Calendar.getInstance();
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
                        String censoredText = TextCensor.censorBannedWords(tweet.getText());
                        StringBuilder keyValue = new StringBuilder();
                        StringBuilder outputValue = new StringBuilder();

                        format.setTimeZone(TimeZone.getTimeZone(TweetUtil.TIME_ZONE_UTC_GMT));
                        calendar.setTimeZone(TimeZone.getTimeZone(TweetUtil.TIME_ZONE_UTC_GMT));
                        calendar.setTime(tweet.getCreationTime());
                        keyValue.append(tweet.getUser().getUserId()).append(UNDERSCORE);
                        keyValue.append(format.format(calendar.getTime()));

                        outputValue.append(tweet.getImpactScore()).append(UNDERSCORE);
                        outputValue.append(tweet.getTweetId()).append(UNDERSCORE);
                        /* Text is printed in Hex to avoid encoding problems */
                        outputValue.append(Hex.encodeHexString(censoredText.getBytes("UTF-8")));

                        context.write(new Text(keyValue.toString()), new Text(outputValue.toString()));
                    }
                }
            } catch (JsonSyntaxException e) {
            } catch (ParseException e) {
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        /**
         * The Reducer method iterates over the values with same key (userId and
         * date). Then it sorts them. Finally it print first the 10 best
         * positive and 10 worst negative.
         */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> uniqueTweetIds = new HashSet<String>();
            ArrayList<Q3Object> posTweets = new ArrayList<Q3Object>();
            ArrayList<Q3Object> negTweets = new ArrayList<Q3Object>();

            String[] keyParts = key.toString().split(UNDERSCORE);
            String userId = keyParts[0];
            String date = keyParts[1];

            /* Iterate over the values with the same key */
            for (Text value : values) {
                String[] valueParts = value.toString().split(UNDERSCORE);
                String impactScore = valueParts[0];
                String tweetId = valueParts[1];
                String text = valueParts[2];

                /* If the tweet id is unique add a new object */
                if (!uniqueTweetIds.contains(tweetId)) {
                    Q3Object q3 = new Q3Object(impactScore, tweetId, text);

                    uniqueTweetIds.add(tweetId);
                    if (q3.getImpactScore() > 0) {
                        posTweets.add(q3);
                    } else {
                        negTweets.add(q3);
                    }
                }
            }

            /*
             * Sort the objects according to the sorting criteria (see Q3Object)
             */
            Collections.sort(posTweets);
            Collections.sort(negTweets);

            int count = 0;
            /* Keep at most 10 positive tweets per user and date */
            for (Q3Object posTweet : posTweets) {
                count++;
                if (count > 10) {
                    break;
                }
                StringBuilder outputValue = new StringBuilder();

                outputValue.append(date).append(TAB);
                outputValue.append(posTweet.getImpactScore()).append(TAB);
                outputValue.append(posTweet.getTweetId()).append(TAB);
                outputValue.append(posTweet.getText()).append(TAB);
                context.write(new Text(userId), new Text(outputValue.toString()));
            }
            count = 0;
            /* Keep at most 10 negative tweets per user and date */
            for (Q3Object negTweet : negTweets) {
                count++;
                if (count > 10) {
                    break;
                }
                StringBuilder outputValue = new StringBuilder();

                outputValue.append(date).append(TAB);
                outputValue.append(negTweet.getImpactScore()).append(TAB);
                outputValue.append(negTweet.getTweetId()).append(TAB);
                outputValue.append(negTweet.getText()).append(TAB);
                context.write(new Text(userId), new Text(outputValue.toString()));
            }
        }
    }

    /**
     * The MAIN method serves as a driver of the custom job.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "q3");

        job.setJarByClass(Q3.class);
        /* Set output keys for mapper and reducer */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        /* Set input and output file formats */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
