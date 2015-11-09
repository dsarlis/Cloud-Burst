package org.cloudburst.etl;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.gson.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.util.*;

public class Q3 {
    private static final String TAB = "\t";
    private static final String UNDERSCORE = "_";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                JsonElement jsonElement = TweetUtil.throwExceptionForMalformedTweets(line);
                Tweet tweet = TweetUtil.generateTweet(jsonElement);

                if (tweet != null) {
                    TextSentimentGrader.addSentimentScore(tweet);
                    if (tweet.getImpactScore() != 0) {
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
                        format.setTimeZone(TimeZone.getTimeZone(TweetUtil.TIME_ZONE_UTC_GMT));
                        StringBuilder outputValue = new StringBuilder();
                        outputValue.append(tweet.getImpactScore()).append(UNDERSCORE);
                        outputValue.append(tweet.getTweetId()).append(UNDERSCORE);
                        outputValue.append(Hex.encodeHex(TextCensor.censorBannedWords(tweet.getText()).getBytes("UTF-8")));
                        String date = format.format(Date.parse(String.valueOf(tweet.getCreationTime())));
                        context.write(new Text(tweet.getUser().getUserId() + UNDERSCORE + date),
                                new Text(tweet.toString()));
                    }
                }
            } catch (JsonSyntaxException e) {
            } catch (ParseException e) {
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashSet<String> uniqueTweetIds = new HashSet<String>();
            ArrayList<Q3Object> posTweets = new ArrayList<Q3Object>();
            ArrayList<Q3Object> negTweets = new ArrayList<Q3Object>();

            String[] keyParts = key.toString().split(UNDERSCORE);

            for (Text value : values) {
                String[] parts = value.toString().split(UNDERSCORE);
                if (!uniqueTweetIds.contains(parts[1])) {
                    uniqueTweetIds.add(parts[1]);
                    Q3Object q3 = new Q3Object(parts[0], parts[1], parts[2]);
                    if (Integer.parseInt(parts[0]) > 0) {
                        posTweets.add(q3);
                    } else {
                        negTweets.add(q3);
                    }
                }
            }

            Collections.sort(posTweets);
            Collections.sort(negTweets, Collections.reverseOrder());

            int count = 0;
            for (Q3Object p: posTweets) {
                if (count > 10) {
                    break;
                }
                StringBuilder outputValue = new StringBuilder();
                outputValue.append(keyParts[1]).append(TAB);
                outputValue.append(p.getImpactScore()).append(TAB);
                outputValue.append(p.getTweetId()).append(TAB);
                outputValue.append(p.getText()).append(TAB);
                count++;
                context.write(new Text(keyParts[0]), new Text(outputValue.toString()));
            }
            count = 0;
            for (Q3Object n: negTweets) {
                if (count > 10) {
                    break;
                }
                StringBuilder outputValue = new StringBuilder();
                outputValue.append(keyParts[1]).append(TAB);
                outputValue.append(n.getImpactScore()).append(TAB);
                outputValue.append(n.getTweetId()).append(TAB);
                outputValue.append(n.getText()).append(TAB);
                count++;
                context.write(new Text(keyParts[0]), new Text(outputValue.toString()));
            }
        }
    }
    /**
     * Main method. It will process all tweet files, read them, insert them into MySQL and create and output file.
     * It can be done in parts:
     * First argument is from, second to, and third the file prefix.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "q3");

        job.setJarByClass(Q3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

