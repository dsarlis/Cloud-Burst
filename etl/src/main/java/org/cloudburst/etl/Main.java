package org.cloudburst.etl;

import java.io.*;
import java.text.ParseException;
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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Main class to process tweets and insert them into MySQL and create an output file.
 */
public class Main {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			JsonElement jsonElement = throwExceptionForMalformedTweets(line);
			Tweet tweet = generateTweet(jsonElement);

			try {
				if (tweet != null && !isTweetOld(tweet)) {
                    TextSentimentGrader.addSentimentScore(tweet);
					context.write(new Text(tweet.getTweetId() + ""), new Text(tweet.toString()));
				}
			} catch (ParseException e) {}
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
	/**
	 * Main method. It will process all tweet files, read them, insert them into MySQL and create and output file.
	 * It can be done in parts:
	 * First argument is from, second to, and third the file prefix.
	 */
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

	/**
	 * Read json tweet.
	 */
	private static Tweet generateTweet(JsonElement jsonElement)  {
		JsonObject jsonObject = jsonElement.getAsJsonObject();

		try {
			JsonObject userObject = jsonObject.getAsJsonObject("user");
			JsonArray hashTagsArray = jsonObject.getAsJsonObject("entities").get("hashtags").getAsJsonArray();
			java.util.Map<String, Integer> hashTags = new HashMap<String, Integer>();

			for (JsonElement hashTag : hashTagsArray) {
				String text = hashTag.getAsJsonObject().get("text").getAsString();
				Integer count = hashTags.get(text);

				hashTags.put(text, count != null ? count + 1 : 1);
			}

			return new Tweet(jsonObject.get("id").getAsLong(), userObject.get("id").getAsLong(), userObject.get("followers_count").getAsInt(), jsonObject.get("created_at").getAsString(), jsonObject.get("text").getAsString(), hashTags);
		} catch (Throwable ex) {
			return null;
		}
	}

	/**
	 * Ignore all tweets that have a time stamp prior to Sun, 20 Apr 2014
	 */
	private static boolean isTweetOld(Tweet tweet) throws ParseException {
		DateTime dateTime = new DateTime(2014, 4, 20, 0, 0, 0, 0, DateTimeZone.UTC);

		return tweet.getCreationTime().before(dateTime.toDate());
	}

	/**
	 * Checks for Malformed Tweets.
	 */
	private static JsonElement throwExceptionForMalformedTweets(String line) throws JsonSyntaxException {
		return new JsonParser().parse(line);
	}

}
