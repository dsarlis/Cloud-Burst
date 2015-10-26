package org.cloudburst.etl.util;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.*;
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
				bannedSetOfWords.add(getROT13DecryptedWord(line.trim()).toLowerCase());
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

	private static void censorWord(StringBuilder word, StringBuilder censoredContent) {
		if (bannedSetOfWords.contains(word.toString().toLowerCase())) {
			StringBuilder censoredWord = new StringBuilder();
			for (int index = 0; index < word.length(); index++) {
				if (index == 0 || index == word.length() - 1) {
					censoredWord.append(word.charAt(index));
				} else {
					censoredWord.append('*');
				}
			}
			censoredContent.append(censoredWord);
		} else {
			censoredContent.append(word);
		}
	}

	/**
	 * Censor checks on the tweet. Example : CENSOR becomes C****R.
	 */
	public static String censorBannedWords(Tweet tweet) {
		String content = tweet.getText();
		StringBuilder word = new StringBuilder();
		StringBuilder censoredContent = new StringBuilder();
		for (int i = 0; i < content.length(); i++) {
			if (Character.isLetterOrDigit(content.charAt(i))) {
				word.append(content.charAt(i));
				if (i == content.length() - 1) {
					censorWord(word, censoredContent);
				}
			} else {
				if (word.length() > 0) {
					censorWord(word, censoredContent);
					word = new StringBuilder();
				}
				censoredContent.append(content.charAt(i));
			}
 		}
		return censoredContent.toString();
	}
}
