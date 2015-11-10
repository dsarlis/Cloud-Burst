package org.cloudburst.etl;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.google.gson.*;
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

/**
 * Main class to process tweets and insert them into MySQL and create an output file.
 */
public class Main {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				String line = value.toString();
				JsonElement jsonElement = TweetUtil.throwExceptionForMalformedTweets(line);
				Tweet tweet = TweetUtil.generateTweet(jsonElement);

				if (tweet != null && !TweetUtil.isTweetOld(tweet)) {
					TextSentimentGrader.addSentimentScore(tweet);
					String censoredText = TextCensor.censorBannedWords(tweet.getText());

					tweet.setText(censoredText);
					context.write(new Text(tweet.getTweetId() + ""), new Text(tweet.toString()));
				}
			} catch (ParseException e) {
			} catch (JsonSyntaxException e) {}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
				break;
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "etl");

		job.setJarByClass(Main.class);
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
