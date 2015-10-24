package org.cloudburst.etl;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.cloudburst.etl.services.MySQLService;
import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.AWSManager;
import org.cloudburst.etl.util.MySQLConnectionFactory;
import org.cloudburst.etl.util.TextCensor;
import org.cloudburst.etl.util.TextSentimentGrader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	private final static int THREAD_NUMBER = Runtime.getRuntime().availableProcessors();

	public static void main(String[] args) throws InterruptedException {
		TweetsDataStoreService tweetsDataStoreService = new TweetsDataStoreService(new AWSManager());
		List<String> tweetFileNames = tweetsDataStoreService.getTweetFileNames();
		Queue<String> fileNamesQueue = new ConcurrentLinkedQueue<>(tweetFileNames);

		Properties boneCPConfigProperties = new Properties();
		try {
			boneCPConfigProperties.load(Main.class.getResourceAsStream("/bonecp.properties"));
		} catch (IOException ex) {
			logger.error("Problem reading properties", ex);
		}

		MySQLConnectionFactory.init(boneCPConfigProperties);
		try {
			TextSentimentGrader.init();
			TextCensor.init();
		} catch (IOException ex) {
			logger.error("Problem reading text-processing files!", ex);
		}

		Worker[] workers = new Worker[THREAD_NUMBER];
		MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());

		for (int i = 0; i < THREAD_NUMBER; i++) {
			workers[i] = new Worker(fileNamesQueue, tweetsDataStoreService, mySQLService);
		}

		for (Worker worker : workers) {
			worker.start();
		}

		for (Worker worker : workers) {
			worker.join();
		}

		MySQLConnectionFactory.shutdown();
	}

}
