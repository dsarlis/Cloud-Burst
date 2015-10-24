package org.cloudburst.etl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.services.MySQLService;
import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.MySQLConnectionFactory;
import org.cloudburst.etl.util.TextCensor;
import org.cloudburst.etl.util.TextSentimentGrader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class Worker implements Runnable {

	private final static Logger logger = LoggerFactory.getLogger(Worker.class);

	private Queue<String> fileNamesQueue;
	private TweetsDataStoreService tweetsDataStoreService;

	static Gson gson = new Gson();
	static Set<Long> uniqueTweetIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

	public Worker(Queue<String> fileNamesQueue, TweetsDataStoreService tweetsDataStoreService) {
		this.fileNamesQueue = fileNamesQueue;
		this.tweetsDataStoreService = tweetsDataStoreService;
	}

	public void run() {
		while (fileNamesQueue.size() > 0) {
			String fileName = fileNamesQueue.poll();

			if (fileName != null) {
				InputStream inputStream = null;
				try {
					inputStream = new FileInputStream("/Users/walia-mac/Downloads/inputSet/chunkai");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				String line = null;

				/* TODO: Discuss proper instantiation with @Fred */
				MySQLService sqlService = new MySQLService(new MySQLConnectionFactory());

				/* TODO: Do this before any file read starts. */
				try {
					TextSentimentGrader.init();
					TextCensor.init();
				} catch (IOException e) {
					logger.error("Problem reading text-processing files!", e);
					e.printStackTrace();
				}

				try {
					while ((line = reader.readLine()) != null) {
						filterAndInsertTweet(line);
					}

				} catch (IOException | ParseException ex) {
					logger.error("Problem reading object", ex);
				}
			}
		}
	}

	private static void filterAndInsertTweet(String line) throws FileNotFoundException, ParseException, IOException {
		try {
			/* Checks Malformed Tweets */
			JsonElement jsonElement = throwExceptionForMalformedTweets(line);

			Tweet tweet = gson.fromJson(jsonElement.getAsJsonObject().toString(), Tweet.class);

			/* Checks redundant Tweets */
			if (!uniqueTweetIds.contains(tweet.getTweetId()) && !isTweetOld(tweet)) {

				uniqueTweetIds.add(tweet.getTweetId());

				TextSentimentGrader.addSentimentScore(tweet);

				System.out.println(tweet);

				/* TODO:Text-censoring almost done (Just left with Asterisks) */
				// TextCensor.censorBannedWords(tweet);

				/* TODO: Let us handle these in batch */
				// sqlService.insertTweet(tweet);

				/* TODO: @Fred Implement storeInBucket */
				// AWSManager.storeInBucket(tweet);
			}
		} catch (JsonSyntaxException ex) {
			/* Eat up the exception to speed up. */
		}
	}

	/**
	 * Ignore all tweets that have a time stamp prior to Sun, 20 Apr 2014
	 */
	private static boolean isTweetOld(Tweet tweet) throws ParseException {
		DateTime dateTime = new DateTime(2014, 4, 20, 0, 0, 0, 0, DateTimeZone.UTC);
		return (dateTime.toDate().before(tweet.getCreationTime())) ? false : true;
	}

	/**
	 * Checks for Malformed Tweets.
	 */
	private static JsonElement throwExceptionForMalformedTweets(String line) throws JsonSyntaxException {
		return new JsonParser().parse(line);
	}
}
