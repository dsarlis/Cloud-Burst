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

public class Q4 {
    private static final String TAB = "\t";
    private static final String UNDERSCORE = "_";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                JsonElement jsonElement = TweetUtil.throwExceptionForMalformedTweets(line);
                Tweet tweet = TweetUtil.generateTweet(jsonElement);

                if (tweet != null && tweet.getHashTags().size() != 0) {
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
                    format.setTimeZone(TimeZone.getTimeZone(TweetUtil.TIME_ZONE_UTC_GMT));
                    String date = format.format(Date.parse(String.valueOf(tweet.getCreationTime())));
                    java.util.Map<String, Integer> hashtags = tweet.getHashTags();
                    for (String hashtag: hashtags.keySet()) {
                        StringBuilder outputValue = new StringBuilder();
                        outputValue.append(hashtags.get(hashtag).intValue()).append(UNDERSCORE);
                        outputValue.append(tweet.getUser().getUserId()).append(UNDERSCORE);
                        outputValue.append(tweet.getCreationTime()).append(UNDERSCORE);
                        outputValue.append(Hex.encodeHex(tweet.getText().getBytes("UTF-8")));
                        context.write(new Text(Hex.encodeHex(hashtag.getBytes("UTF-8")) + UNDERSCORE + date),
                                new Text(outputValue.toString()));
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

            String[] keyParts = key.toString().split(UNDERSCORE);

            long finalCount = 0;
            HashSet<Long> usersList = new HashSet<Long>();
            ArrayList<Q4Object> q4 = new ArrayList<Q4Object>();
            for (Text value : values) {
                String[] parts = value.toString().split(UNDERSCORE);
                finalCount += Long.parseLong(parts[0]);
                usersList.add(Long.parseLong(parts[1]));
                q4.add(new Q4Object(parts[2], parts[3]));
            }
            Collections.sort(q4);
            ArrayList<Long> uniqueUsersList = new ArrayList<Long>(usersList);
            Collections.sort(uniqueUsersList);

            StringBuilder outputValue = new StringBuilder();
            outputValue.append(keyParts[1]).append(TAB);
            outputValue.append(finalCount).append(TAB);
            outputValue.append(uniqueUsersList).append(TAB);
            outputValue.append(q4.get(0).getText());
            context.write(new Text(keyParts[0]), new Text(outputValue.toString()));
        }
    }
    /**
     * Main method. It will process all tweet files, read them, insert them into MySQL and create and output file.
     * It can be done in parts:
     * First argument is from, second to, and third the file prefix.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "q4");

        job.setJarByClass(Q4.class);
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

