package org.cloudburst.etl;

import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
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
import org.cloudburst.etl.util.TweetUtil;

import java.io.IOException;
import java.util.*;

public class Q5 {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String line = value.toString();
                JsonElement jsonElement = TweetUtil.throwExceptionForMalformedTweets(line);
                Tweet tweet = TweetUtil.generateTweet(jsonElement);

                if (tweet != null) {
                    context.write(new Text(tweet.getUser().getUserId() + ""), new Text(tweet.getTweetId() + ""));
                }
            } catch (JsonSyntaxException e) {
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashSet<String> uniqueTweetIds = new HashSet<String>();
            /* Iterate over the values with the same key */
            for (Text value : values) {
                uniqueTweetIds.add(value.toString());
            }
            context.write(key, new Text(uniqueTweetIds.size() + ""));
        }
    }

    /**
     * The MAIN method serves as a driver of the custom job.
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "q5");

        job.setJarByClass(Q5.class);
        /* Set output keys for mapper and reducer */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        /* Set input and output file formats */
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
