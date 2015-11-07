package org.cloudburst.etl;

import java.io.*;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.gson.*;
import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.TextCensor;
import org.cloudburst.etl.util.TextSentimentGrader;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Thread that will read file, filter tweets, insert them into MySQL and write them into a file.
 */
public class Worker implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(Worker.class);

	private String fileName;
	private TweetsDataStoreService tweetsDataStoreService;
	/**
	 * Sets to avoid using same tweet id.
	 */
	private Set<Long> uniqueTweetIds;
	/**
	 * Count how many threads are done.
	 */
	private AtomicInteger counter;
	private String pathToFile;

	public Worker(String fileName, TweetsDataStoreService tweetsDataStoreService, Set<Long> uniqueTweetIds, AtomicInteger counter, String pathToFile) {
		this.fileName = fileName;
		this.tweetsDataStoreService = tweetsDataStoreService;
		this.uniqueTweetIds = uniqueTweetIds;
		this.counter = counter;
		this.pathToFile = pathToFile;
	}

	@Override
	public void run() {
		String downloadedFileName = getDownloadedFileName(fileName);
		String processedFileName = "out-" + downloadedFileName;

		downloadedFileName = pathToFile + downloadedFileName;
		downloadFile(fileName, downloadedFileName);
		logger.info("Processing file {}", downloadedFileName);
		try  {
			processFile(processedFileName, downloadedFileName);
		} catch (Throwable ex) {
			logger.error("Problem processing file=" + downloadedFileName, ex);
		}
		logger.info("Done processing file {}", downloadedFileName);
		deleteFile(downloadedFileName);
		counter.incrementAndGet();
	}

	/**
	 * Download tweet file from s3.
	 */
	private void downloadFile(String fileName, String downloadedFileName) {
		String line = null;

		logger.info("Downloading file={}", fileName);
		try (BufferedReader s3Reader = new BufferedReader(new InputStreamReader(tweetsDataStoreService.getTweetFileInputStream(fileName)));
			 FileOutputStream inputFileOutputStream = new FileOutputStream(downloadedFileName)) {
			while ((line = s3Reader.readLine()) != null) {
				line = line + "\n";
				inputFileOutputStream.write(line.getBytes());
			}
		} catch (IOException ex) {
			logger.error("Problem downloading file=" + fileName, ex);
		}
		logger.info("Done downloading file={}", fileName);
	}

	/**
	 * Process file: ignore tweets, insert into MySQL correct ones and write them to a file.
	 */
	private void processFile(String processedFileName, String downloadedFileName) {
		String line;
		int bigCount = 1;
		logger.info("Reading file={}", downloadedFileName);
		try (BufferedReader inputReader = new BufferedReader(new FileReader(downloadedFileName));
             FileOutputStream fileOutputStream = new FileOutputStream(processedFileName)) {
             while ((line = inputReader.readLine()) != null) {
                filterAndInsertTweet(fileOutputStream, line);
                if (bigCount % 50000 == 0) {
                    logger.info("file={}, count={}", downloadedFileName, bigCount);
                }
                bigCount++;
             }
        } catch (IOException | ParseException ex) {
            logger.error("Problem reading file=" + downloadedFileName, ex);
        }
		logger.info("Done reading file={}", downloadedFileName);
	}

	/**
	 * Delete file.
	 */
	private void deleteFile(String fileName) {
		logger.info("Deleting file={}", fileName);
		File outputFile = new File(fileName);

		outputFile.delete();
		logger.info("Done deleting file={}", fileName);
	}

	private String getDownloadedFileName(String fileName) {
		String[] tokens = fileName.split("/");

		return tokens.length > 0 ? tokens[tokens.length - 1] : fileName;
	}

	/**
	 * Check if tweet is valid and process it.
	 */
	private void filterAndInsertTweet(FileOutputStream fileOutputStream, String line) throws ParseException, IOException {
		try {
			/* Checks Malformed Tweets */
			JsonElement jsonElement = throwExceptionForMalformedTweets(line);
			Tweet tweet = generateTweet(jsonElement);

			/* Checks redundant Tweets */
			if (tweet != null && !uniqueTweetIds.contains(tweet.getTweetId()) && !isTweetOld(tweet)) {
				uniqueTweetIds.add(tweet.getTweetId());
				TextSentimentGrader.addSentimentScore(tweet);
				fileOutputStream.write(tweet.toString().getBytes());
			}
		} catch (JsonSyntaxException ex) {
			/* Eat up the exception to speed up. */
		}
	}

	/**
	 * Read json tweet.
	 */
	private Tweet generateTweet(JsonElement jsonElement)  {
		JsonObject jsonObject = jsonElement.getAsJsonObject();

		try {
			JsonObject userObject = jsonObject.getAsJsonObject("user");
			JsonArray hashTagsArray = jsonObject.getAsJsonObject("entities").get("hashtags").getAsJsonArray();
			Map<String, Integer> hashTags = new HashMap<String, Integer>();

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
	private boolean isTweetOld(Tweet tweet) throws ParseException {
		DateTime dateTime = new DateTime(2014, 4, 20, 0, 0, 0, 0, DateTimeZone.UTC);

		return tweet.getCreationTime().before(dateTime.toDate());
	}

	/**
	 * Checks for Malformed Tweets.
	 */
	private JsonElement throwExceptionForMalformedTweets(String line) throws JsonSyntaxException {
		return new JsonParser().parse(line);
	}

}
