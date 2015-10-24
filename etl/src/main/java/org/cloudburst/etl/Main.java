package org.cloudburst.etl;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.AWSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	private final static int THREAD_NUMBER = Runtime.getRuntime().availableProcessors();

	public static void main(String[] args) throws ParseException, FileNotFoundException, IOException {
		TweetsDataStoreService tweetsDataStoreService = new TweetsDataStoreService(new AWSManager());
		List<String> tweetFileNames = tweetsDataStoreService.getTweetFileNames();
		Queue<String> fileNamesQueue = new ConcurrentLinkedQueue<>(tweetFileNames);

		// Start workers
	}
}
