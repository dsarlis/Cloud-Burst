package org.cloudburst.etl;

import java.io.*;
import java.text.ParseException;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.services.MySQLService;
import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.TextSentimentGrader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

public class Worker extends Thread {

	private final static Logger logger = LoggerFactory.getLogger(Worker.class);

	private Queue<String> fileNamesQueue;
	private TweetsDataStoreService tweetsDataStoreService;
	private MySQLService mySQLService;

	static Gson gson = new Gson();
	static Set<Long> uniqueTweetIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

	public Worker(Queue<String> fileNamesQueue, TweetsDataStoreService tweetsDataStoreService, MySQLService mySQLService) {
		this.fileNamesQueue = fileNamesQueue;
		this.tweetsDataStoreService = tweetsDataStoreService;
		this.mySQLService = mySQLService;
	}

	@Override
	public void run() {
		while (fileNamesQueue.size() > 0) {
			String fileName = fileNamesQueue.poll();

			if (fileName != null) {
				logger.info("Reading file {}", fileName);
				InputStream inputStream = null;

				try {
					inputStream = new FileInputStream("/Users/walia-mac/Downloads/inputSet/chunkai");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

				BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
				String line = null;

				try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
					while ((line = reader.readLine()) != null) {
						filterAndInsertTweet(fileOutputStream, line);
					}
				} catch (IOException | ParseException ex) {
					logger.error("Problem reading object or file", ex);
				}

				//TODO:Test and check bucket name, correct file name, correct upload, etc.
				tweetsDataStoreService.saveTweetsFile(fileName);
				logger.info("Done with file {}", fileName);
				/*
				I left you logic so you can finish testing, but it should be something like this.

				try(BufferedReader reader = new BufferedReader(new InputStreamReader(tweetsDataStoreService.getTweetFileInputStream(fileName)))) {
					String line = null;

					try (FileOutputStream fileOutputStream = new FileOutputStream(fileName)) {
						while ((line = reader.readLine()) != null) {
							filterAndInsertTweet(line);
						}

					} catch (IOException | ParseException ex) {
						logger.error("Problem reading object or file", ex);
					}

					tweetsDataStoreService.saveTweetsFile(fileName);

				} catch (IOException ex) {
					logger.error("Problem reading file", ex);
				}
				 */
			}
		}
	}

	private void filterAndInsertTweet(FileOutputStream fileOutputStream, String line) throws ParseException, IOException {
		try {
			/* Checks Malformed Tweets */
			JsonElement jsonElement = throwExceptionForMalformedTweets(line);
			Tweet tweet = gson.fromJson(jsonElement.getAsJsonObject().toString(), Tweet.class);

			/* Checks redundant Tweets */
			if (!uniqueTweetIds.contains(tweet.getTweetId()) && !isTweetOld(tweet)) {
				uniqueTweetIds.add(tweet.getTweetId());
				//TODO: Suggestion: avoid using static methods. I think there is no need for it here.
				TextSentimentGrader.addSentimentScore(tweet);

				fileOutputStream.write(tweet.toString().getBytes());

				/* TODO:Text-censoring almost done (Just left with Asterisks) */
				// TextCensor.censorBannedWords(tweet);

				/* TODO: Let us handle these in batch
				*
				* The performance difference is not that much:
				* http://stackoverflow.com/questions/11389449/performance-of-mysql-insert-statements-in-java-batch-mode-prepared-statements-v
				*
				* We are using connection pool, it should be fast.
				*
				* Prefer to keep it simple to avoid errors.
				*
				* */
				mySQLService.insertTweet(tweet);
			}
		} catch (JsonSyntaxException ex) {
			/* Eat up the exception to speed up. */
		}
	}

	/**
	 * Ignore all tweets that have a time stamp prior to Sun, 20 Apr 2014
	 */
	private boolean isTweetOld(Tweet tweet) throws ParseException {
		DateTime dateTime = new DateTime(2014, 4, 20, 0, 0, 0, 0, DateTimeZone.UTC);

		return !dateTime.toDate().before(tweet.getCreationTime());
	}

	/**
	 * Checks for Malformed Tweets.
	 */
	private JsonElement throwExceptionForMalformedTweets(String line) throws JsonSyntaxException {
		return new JsonParser().parse(line);
	}
}
