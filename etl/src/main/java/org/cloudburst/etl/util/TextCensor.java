package org.cloudburst.etl.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.cloudburst.etl.model.Tweet;

public class TextCensor {

	private static final String BANNED_LIST_LOC = "src/main/resources/banned.txt";

	/* Keeping a SET at class level. (Size of keySet = 390) */
	private final static Set<String> bannedSetOfWords = Collections
			.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

	/**
	 * Populates in-memory needs for text processing.
	 */
	public static void init() throws FileNotFoundException, IOException {
		populateBannedSetOfWords();
	}

	/**
	 * TEXT-CENSORING : Reads the files that contains ROT-13 version of all
	 * banned words.
	 */
	private static void populateBannedSetOfWords() throws IOException, FileNotFoundException {
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(BANNED_LIST_LOC)));) {
			String line = null;
			while ((line = reader.readLine()) != null) {
				bannedSetOfWords.add(getROT13DecryptedWord(line.trim()));
			}
		}
	}

	/**
	 * Decrypting using the ROT-13 algorithm.
	 */
	private static String getROT13DecryptedWord(String word) {
		StringBuilder charSet = new StringBuilder();
		for (Character ch : word.toUpperCase().toCharArray()) {
			if (Character.isAlphabetic(ch)) {
				ch = (char) ((ch < 78) ? ch + 13 : ch - 13);
			}
			charSet.append(ch);
		}
		return charSet.toString();
	}

	/**
	 * Censor checks on the tweet. Example : CENSOR becomes C****R.
	 */
	public static String censorBannedWords(Tweet tweet) {
		String[] chunks = tweet.getText().split("[^A-Za-z0-9]");
		for (String chunk : chunks) {
			if (!chunk.isEmpty() && bannedSetOfWords.contains(chunk)) {
				/* TODO : Make words with asterisk. */
			}
		}
		return ""; /* TODO: Return updated tweet text. */
	}
}
