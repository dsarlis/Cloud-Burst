package org.cloudburst.etl;

import java.io.*;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.cloudburst.etl.services.MySQLService;
import org.cloudburst.etl.services.TweetsDataStoreService;
import org.cloudburst.etl.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private final static int THREAD_NUMBER = Runtime.getRuntime().availableProcessors();
	private final static long TWO_MINUTES = 120000;
	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	private final static ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUMBER);
	private final static AtomicInteger counter = new AtomicInteger(1);
	private static final String QUEUE_FILENAME = "queue.object";

	public static void main(String[] args) throws InterruptedException, SQLException {
		LoggingConfigurator.configureFor(LoggingConfigurator.Environment.PRODUCTION);
		TweetsDataStoreService tweetsDataStoreService = new TweetsDataStoreService(new AWSManager());
		List<String> tweetFileNames = tweetsDataStoreService.getTweetFileNames();

		if (args.length == 2) {
			tweetFileNames = tweetFileNames.subList(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
		}
		initStructures();

		Set<Long> uniqueTweetIds = getTweetIdsSet();

		for (String tweetFileName : tweetFileNames) {
			MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());
			Worker worker = new Worker(tweetFileName, tweetsDataStoreService, mySQLService, uniqueTweetIds, counter);

			threadPool.execute(worker);
		}

		while (counter.get() < tweetFileNames.size()) {
			logger.info("{} threads done", counter.get());
			Thread.sleep(TWO_MINUTES);
		}

		writeTweetIdsSet(uniqueTweetIds);
		MySQLConnectionFactory.shutdown();
	}

	private static void writeTweetIdsSet(Set<Long> uniqueTweetIds) {
		try (FileOutputStream classFile = new FileOutputStream(QUEUE_FILENAME);
			 ObjectOutputStream objectOutputStream = new ObjectOutputStream(classFile)) {

			objectOutputStream.writeObject(uniqueTweetIds);
		} catch (FileNotFoundException ex) {
			logger.error("File not found");
		} catch (IOException ex) {
			logger.error("Problem writing class", ex);
		}
	}

	private static void initStructures() {
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
	}

	private static Set<Long> getTweetIdsSet() {
		Set<Long> uniqueTweetIds = null;

		try(FileInputStream classFile = new FileInputStream(QUEUE_FILENAME);
			ObjectInputStream objectInputStream = new ObjectInputStream(classFile)) {

			uniqueTweetIds = (Set<Long>) objectInputStream.readObject();
		} catch (FileNotFoundException ex) {
			uniqueTweetIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
		} catch (IOException ex) {
			logger.error("Problem reading file", ex);
			System.exit(-1);
		} catch (ClassNotFoundException ex) {
			logger.error("Call problem", ex);
			System.exit(-1);
		}
		return uniqueTweetIds;
	}

}
