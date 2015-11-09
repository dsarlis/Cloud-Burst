package org.cloudburst.etl.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.cloudburst.etl.model.Tweet;

public class TextSentimentGrader {

	private static final String AFINN_FILE_NAME = "/afinn.txt";

	private static final Pattern REGEX_NON_ALPHA_NUM = Pattern.compile("[^A-Za-z0-9]");
	private static final String TAB = "\t";

	/* Keeping a MAP at class level. (Size of keySet = 2475) */
	private final static Map<String, Integer> afinnPool = new ConcurrentHashMap<String, Integer>();

	/**
	 * Populates in-memory needs for text processing.
	 */
	static {
		try {
			populateSentimentMap();
		} catch (IOException e) {}
	}

	/**
	 * SENTIMENT-SCORING : Reads the file that contains sentiment scores for
	 * words as labeled by Finn Årup Nielsen in 2009-2011.
	 */
	private static void populateSentimentMap() throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(TextSentimentGrader.class.getResourceAsStream(AFINN_FILE_NAME)));
		String line = null;

		while ((line = reader.readLine()) != null) {
			String[] chunks = line.split(TAB);
			afinnPool.put(chunks[0], Integer.valueOf(chunks[1]));
		}
	}

	/**
	 * Calculates the total sentiment score for a tweet.
	 */
	public static void addSentimentScore(Tweet tweet) throws IOException {
		String[] chunks = REGEX_NON_ALPHA_NUM.split(tweet.getText());

		for (String textChunk : chunks) {
			if (!textChunk.isEmpty()) {
				String key = textChunk.toLowerCase();

				if (afinnPool.containsKey(key)) {
					tweet.adjustScore(afinnPool.get(key));
				}
			}
		}
	}

}
