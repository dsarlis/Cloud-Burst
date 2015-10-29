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

/**
 * Main class to process tweets and insert them into MySQL and create an output file.
 */
public class Main {

	private final static int THREAD_NUMBER = Runtime.getRuntime().availableProcessors();
	private final static long TWO_MINUTES = 120000;
	private final static Logger logger = LoggerFactory.getLogger(Main.class);
	private final static ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_NUMBER);
	private final static AtomicInteger counter = new AtomicInteger(0);
	private static final String QUEUE_FILENAME = "queue.object";

	/**
	 * Method to create threads that will transform one file into another.
	 */
	private static void transformFiles(String directoryFileName) throws InterruptedException {
		File directory = new File(directoryFileName);
		File[] files = directory.listFiles();

		if (files != null) {
			List<String> tweetFileNames = new ArrayList<String>();

			for (File file : files) {
				tweetFileNames.add(file.getName());
			}
			for (int chunk = 0; chunk < tweetFileNames.size(); chunk += THREAD_NUMBER) {
				int to = chunk + THREAD_NUMBER < tweetFileNames.size() ? chunk + THREAD_NUMBER : tweetFileNames.size();
				for (String tweetFileName : tweetFileNames.subList(chunk, to)) {
					TransformerWorker worker = new TransformerWorker(tweetFileName, counter);

					threadPool.execute(worker);
				}
				while (counter.get() < to) {
					logger.info("done {}/{}", counter.get(), to);
					Thread.sleep(TWO_MINUTES/2);
				}
				logger.info("done {}/{}", counter.get(), files.length);
			}
			threadPool.shutdown();
		}
	}

	/**
	 * Method to only insert into MySQL.
	 */
	private static void insertFiles(String directoryFileName) throws InterruptedException, SQLException {
		File directory = new File(directoryFileName);
		File[] files = directory.listFiles();

		if (files != null) {
			Properties boneCPConfigProperties = new Properties();
			try {
				boneCPConfigProperties.load(Main.class.getResourceAsStream("/bonecp.properties"));
			} catch (IOException ex) {
				logger.error("Problem reading properties", ex);
			}

			MySQLConnectionFactory.init(boneCPConfigProperties);
			List<String> tweetFileNames = new ArrayList<String>();
			Arrays.sort(files);

			for (File file : files) {
				tweetFileNames.add(file.getName());
			}

			Set<Long> uniqueTweetIds = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
			for (int chunk = 0; chunk < tweetFileNames.size(); chunk += THREAD_NUMBER) {
				int to = chunk + THREAD_NUMBER < tweetFileNames.size() ? chunk + THREAD_NUMBER : tweetFileNames.size();
				for (String tweetFileName : tweetFileNames.subList(chunk, to)) {
					MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());
					NewWorker worker = new NewWorker(tweetFileName, mySQLService, counter, directoryFileName, uniqueTweetIds);

					threadPool.execute(worker);
				}
				while (counter.get() < to) {
					logger.info("done {}/{}", counter.get(), to);
					Thread.sleep(TWO_MINUTES);
				}
				logger.info("done {}/{}", counter.get(), tweetFileNames.size());
			}
			threadPool.shutdown();
		}
	}

	/**
	 * Main method. It will process all tweet files, read them, insert them into MySQL and create and output file.
	 * It can be done in parts:
	 * First argument is from, second to, and third the file prefix.
	 */
	public static void main(String[] args) throws InterruptedException, SQLException {
		LoggingConfigurator.configureFor(LoggingConfigurator.Environment.PRODUCTION);

		TweetsDataStoreService tweetsDataStoreService = new TweetsDataStoreService(new AWSManager());
		List<String> tweetFileNames = tweetsDataStoreService.getTweetFileNames();
		int from = Integer.parseInt(args[0]);
		int to = Integer.parseInt(args[1]);

		tweetFileNames = tweetFileNames.subList(from, to);
		initStructures();

		Set<Long> uniqueTweetIds = getTweetIdsSet();

		for (int chunk = 0; chunk < tweetFileNames.size(); chunk += THREAD_NUMBER) {
			for (String tweetFileName : tweetFileNames.subList(chunk, chunk + THREAD_NUMBER < tweetFileNames.size() ? chunk + THREAD_NUMBER : tweetFileNames.size())) {
				MySQLService mySQLService = new MySQLService(new MySQLConnectionFactory());
				Worker worker = new Worker(tweetFileName, tweetsDataStoreService, mySQLService, uniqueTweetIds, counter, args[2]);

				threadPool.execute(worker);
			}
			while (counter.get() < tweetFileNames.size()) {
				logger.info("done {}/{}", counter.get(), tweetFileNames.size());
				Thread.sleep(TWO_MINUTES);
			}
			logger.info("done {}/{}", counter.get(), tweetFileNames.size());
		}
		if (to < tweetFileNames.size()) {
			writeTweetIdsSet(uniqueTweetIds);
		}
		MySQLConnectionFactory.shutdown();
		threadPool.shutdown();
		logger.info("I am done :)");
	}

	/**
	 * Serialize Tweet Ids set.
	 */
	private static void writeTweetIdsSet(Set<Long> uniqueTweetIds) {
		logger.info("Writing class");
		try (FileOutputStream classFile = new FileOutputStream(QUEUE_FILENAME);
			 ObjectOutputStream objectOutputStream = new ObjectOutputStream(classFile)) {

			objectOutputStream.writeObject(uniqueTweetIds);
		} catch (FileNotFoundException ex) {
			logger.error("File not found");
		} catch (IOException ex) {
			logger.error("Problem writing class", ex);
		}
		logger.info("Done writing class");
	}

	/**
	 * Initialize structures: MySQL, sentiment list and score list.
	 */
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

	/**
	 * Deserialize Tweet Ids set.
	 */
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
