package org.cloudburst.etl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.cloudburst.etl.services.MySQLService;
import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	private final static int THREAD_NUMBER = Runtime.getRuntime().availableProcessors();

	public static void main(String[] args) throws InterruptedException, SQLException {
		LoggingConfigurator.configureFor(LoggingConfigurator.Environment.PRODUCTION);
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

		for (int i = 0; i < THREAD_NUMBER; i++) {
			MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());
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
