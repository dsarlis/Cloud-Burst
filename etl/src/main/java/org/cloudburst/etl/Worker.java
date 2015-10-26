package org.cloudburst.etl;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.services.MySQLService;
import org.cloudburst.etl.services.TweetsDataStoreService;
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

public class Worker extends Thread {

	private static final Logger logger = LoggerFactory.getLogger(Worker.class);
	private static final int BATCH_SIZE = 100;

	private Queue<String> fileNamesQueue;
	private TweetsDataStoreService tweetsDataStoreService;
	private MySQLService mySQLService;
	private List<Tweet> tweets;

	private static Gson gson = new Gson();
	private static Set<Long> uniqueTweetIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());

	public Worker(Queue<String> fileNamesQueue, TweetsDataStoreService tweetsDataStoreService, MySQLService mySQLService) {
		this.fileNamesQueue = fileNamesQueue;
		this.tweetsDataStoreService = tweetsDataStoreService;
		this.mySQLService = mySQLService;
	}

	@Override
	public void run() {
		while (fileNamesQueue.size() > 0) {
			String fileName = fileNamesQueue.poll();
			String outputFileName = getOutputFileName(fileName);

			if (fileName != null) {
				logger.info("Reading file {}", fileName);
				try(BufferedReader reader = new BufferedReader(new InputStreamReader(tweetsDataStoreService.getTweetFileInputStream(fileName)))) {
					String line = null;
					int count = 1;
					tweets = new ArrayList<Tweet>();

					try (FileOutputStream fileOutputStream = new FileOutputStream(outputFileName)) {
						while ((line = reader.readLine()) != null) {
							filterAndInsertTweet(fileOutputStream, line);
							if (count == BATCH_SIZE) {
								mySQLService.insertTweets(tweets);
								tweets = new ArrayList<Tweet>();
								count = 1;
							}
							count++;
						}
						mySQLService.insertTweets(tweets);
					} catch (IOException | ParseException ex) {
						logger.error("Problem reading object or file", ex);
					}

					tweetsDataStoreService.saveTweetsFile(outputFileName);
					deleteFile(outputFileName);
					logger.info("Done with file {}", fileName);
				} catch (IOException ex) {
					logger.error("Problem reading file", ex);
				}
			}
		}
		mySQLService.close();
	}

	private void deleteFile(String outputFileName) {
		File outputFile = new File(outputFileName);

		outputFile.delete();
	}

	private String getOutputFileName(String fileName) {
		String[] tokens = fileName.split("/");

		return tokens.length > 0 ? tokens[tokens.length - 1] : fileName;
	}

	private void filterAndInsertTweet(FileOutputStream fileOutputStream, String line) throws ParseException, IOException {
		try {
			/* Checks Malformed Tweets */
			JsonElement jsonElement = throwExceptionForMalformedTweets(line);
			Tweet tweet = gson.fromJson(jsonElement.getAsJsonObject().toString(), Tweet.class);

			/* Checks redundant Tweets */
			if (!uniqueTweetIds.contains(tweet.getTweetId()) && !isTweetOld(tweet)) {
				uniqueTweetIds.add(tweet.getTweetId());
				TextSentimentGrader.addSentimentScore(tweet);

				fileOutputStream.write(tweet.toString().getBytes());

				TextCensor.censorBannedWords(tweet);

				tweets.add(tweet);
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
