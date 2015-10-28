package org.cloudburst.etl;

import org.cloudburst.etl.model.Tweet;
import org.cloudburst.etl.services.MySQLService;
import org.cloudburst.etl.services.TweetsDataStoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class NewWorker implements Runnable {

	private static final Logger logger = LoggerFactory.getLogger(NewWorker.class);
	private static final int BATCH_SIZE = 1000;

	private String fileName;
	private MySQLService mySQLService;
	private List<Tweet> tweets;
	private AtomicInteger counter;
	private String pathToFile;

	public NewWorker(String fileName, MySQLService mySQLService, AtomicInteger counter, String pathToFile) {
		this.fileName = fileName;
		this.mySQLService = mySQLService;
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
				String[] tokens = line.split("\t");
				StringBuilder textBuilder = new StringBuilder();

				for (int i = 4; i < tokens.length;i++) {
					textBuilder.append(tokens[i]);
				}

				Tweet tweet = new Tweet(Long.parseLong(tokens[0]), Long.parseLong(tokens[1]), tokens[2], textBuilder.toString().replace("\\n", "\n").replace("\\r", "\r"));

				tweets.add(tweet);
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

}
