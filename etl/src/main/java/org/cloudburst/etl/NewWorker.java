package org.cloudburst.etl;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.services.MySQLService;
import org.cloudburst.etl.util.TextCensor;
import org.cloudburst.etl.util.TextSentimentGrader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Worker to only insert into MySQL. Works the same as Worker, but it does not write into a file.
 */
public class NewWorker implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(NewWorker.class);
	private static final int BATCH_SIZE = 1000;

	private String fileName;
	private MySQLService mySQLService;
	private List<Tweet> tweets;
	private AtomicInteger counter;
	private String pathToFile;
	private Set<Long> uniqueTweetIds;

	public NewWorker(String fileName, MySQLService mySQLService, AtomicInteger counter, String pathToFile, Set<Long> uniqueTweetIds) {
		this.fileName = fileName;
		this.mySQLService = mySQLService;
		this.uniqueTweetIds = uniqueTweetIds;
		this.counter = counter;
		this.pathToFile = pathToFile;
	}

	@Override
	public void run() {
		String downloadedFileName = pathToFile + fileName;

		logger.info("Processing file {}", fileName);
		try  {
			processFile(downloadedFileName);
		} catch (Throwable ex) {
			logger.error("Problem processing file=" + fileName, ex);
		}
		logger.info("Done processing file {}", fileName);
		counter.incrementAndGet();
	}

	private void processFile(String downloadedFileName) {
		String line;
		int bigCount = 1;
		int count = 1;
		tweets = new ArrayList<Tweet>(BATCH_SIZE);

		logger.info("Reading file={}", downloadedFileName);
		try (BufferedReader inputReader = new BufferedReader(new FileReader(downloadedFileName))) {
			while ((line = inputReader.readLine()) != null) {
				filterAndInsertTweet(line);
				if (count == BATCH_SIZE) {
					mySQLService.insertTweets(tweets);
					tweets = new ArrayList<Tweet>(BATCH_SIZE);
					count = 1;
				}
				if (bigCount % 50000 == 0) {
					logger.info("file={}, count={}", downloadedFileName, bigCount);
				}
				count++;
				bigCount++;
			}
			mySQLService.insertTweets(tweets);
		} catch (IOException | ParseException ex) {
			logger.error("Problem reading file=" + downloadedFileName, ex);
		}
		logger.info("Done reading file={}", downloadedFileName);
	}

	private void filterAndInsertTweet(String line) throws ParseException, IOException {
		try {
			/* Checks Malformed Tweets */
			JsonElement jsonElement = throwExceptionForMalformedTweets(line);
			Tweet tweet = generateTweet(jsonElement);

			/* Checks redundant Tweets */
			if (tweet != null && !uniqueTweetIds.contains(tweet.getTweetId()) && !isTweetOld(tweet)) {
				uniqueTweetIds.add(tweet.getTweetId());
				TextSentimentGrader.addSentimentScore(tweet);
				TextCensor.censorBannedWords(tweet);
				tweets.add(tweet);
			}
		} catch (JsonSyntaxException ex) {
			/* Eat up the exception to speed up. */
		}
	}

	private Tweet generateTweet(JsonElement jsonElement)  {
		JsonObject jsonObject = jsonElement.getAsJsonObject();

		try {
			return new Tweet(jsonObject.get("id").getAsLong(), jsonObject.getAsJsonObject("user").get("id").getAsLong(), jsonObject.get("created_at").getAsString(), jsonObject.get("text").getAsString());
		} catch (Throwable ex) {
			return null;
		}
	}

	private boolean isTweetOld(Tweet tweet) throws ParseException {
		DateTime dateTime = new DateTime(2014, 4, 20, 0, 0, 0, 0, DateTimeZone.UTC);

		return tweet.getCreationTime().before(dateTime.toDate());
	}

	private JsonElement throwExceptionForMalformedTweets(String line) throws JsonSyntaxException {
		return new JsonParser().parse(line);
	}

}
