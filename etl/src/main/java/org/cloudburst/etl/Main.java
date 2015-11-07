package org.cloudburst.etl;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class to process tweets and insert them into MySQL and create an output file.
 */
public class Main {

	private final static int THREAD_NUMBER = Runtime.getRuntime().availableProcessors();
	private final static long TWO_MINUTES = 120000;
	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	private final static ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUMBER);
	private final static AtomicInteger counter = new AtomicInteger(0);

	/**
	 * Main method. It will process all tweet files, read them, insert them into MySQL and create and output file.
	 * It can be done in parts:
	 * First argument is from, second to, and third the file prefix.
	 */
	public static void main(String[] args) throws InterruptedException, SQLException {
		LoggingConfigurator.configureFor(LoggingConfigurator.Environment.PRODUCTION);
		TweetsDataStoreService tweetsDataStoreService = new TweetsDataStoreService(new AWSManager());
		List<String> tweetFileNames = tweetsDataStoreService.getTweetFileNames();
		Set<Long> uniqueTweetIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
		String pathToFile = args.length > 0 ? args[0] : "!";

		initStructures();
		for (int chunk = 0; chunk < tweetFileNames.size(); chunk += THREAD_NUMBER) {
			for (String tweetFileName : tweetFileNames.subList(chunk, chunk + THREAD_NUMBER < tweetFileNames.size() ? chunk + THREAD_NUMBER : tweetFileNames.size())) {
				Worker worker = new Worker(tweetFileName, tweetsDataStoreService, uniqueTweetIds, counter, pathToFile);

				threadPool.execute(worker);
			}
			while (counter.get() < tweetFileNames.size()) {
				logger.info("done {}/{}", counter.get(), tweetFileNames.size());
				Thread.sleep(TWO_MINUTES);
			}
			logger.info("done {}/{}", counter.get(), tweetFileNames.size());
		}
		threadPool.shutdown();
		logger.info("I am done :)");
	}

	/**
	 * Initialize structures: MySQL, sentiment list and score list.
	 */
	private static void initStructures() {
		try {
			TextSentimentGrader.init();
		} catch (IOException ex) {
			logger.error("Problem reading text-processing files!", ex);
		}
	}

}
