package org.cloudburst.etl.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.cloudburst.etl.model.Tweet;

public class TextSentimentGrader {

	private static final String AFINN_FILE_LOC = "src/main/resources/afinn.txt";

	private static final String REGEX_NON_ALPHA_NUM = "[^A-Za-z0-9]";
	private static final String TAB = "\t";

	/* Keeping a MAP at class level. (Size of keySet = 2475) */
	private final static Map<String, Integer> afinnPool = new ConcurrentHashMap<>();

	/**
	 * Populates in-memory needs for text processing.
	 */
	public static void init() throws FileNotFoundException, IOException {
		populateSentimentMap();
	}

	/**
	 * SENTIMENT-SCORING : Reads the file that contains sentiment scores for
	 * words as labeled by Finn Årup Nielsen in 2009-2011.
	 */
	private static void populateSentimentMap() throws IOException, FileNotFoundException {
		//TODO: maybe use classpath.
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(AFINN_FILE_LOC)))) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				String[] chunks = line.split(TAB);
				afinnPool.put(chunks[0], Integer.valueOf(chunks[1]));
			}
		}
	}

	/**
	 * Calculates the total sentiment score for a tweet.
	 */
	public static void addSentimentScore(Tweet tweet) throws FileNotFoundException, IOException {

		String[] chunks = tweet.getText().split(REGEX_NON_ALPHA_NUM);
		for (String textChunk : chunks) {
			System.out.println(textChunk);
			if (!textChunk.isEmpty()) {
				for (String key : afinnPool.keySet()) {
					if (textChunk.toLowerCase().equals(key)) {
						tweet.adjustScore(afinnPool.get(key));
					}
				}
			}
		}
	}
}
