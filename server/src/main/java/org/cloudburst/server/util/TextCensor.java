package org.cloudburst.server.util;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Class for censoring the banned words in the tweet text.
 */
public class TextCensor {

    private static final String BANNED_LIST_FILE_NAME = "/banned.txt";

    /* Keeping a SET at class level. (Size of keySet = 390) */
    private final static Set<String> bannedSetOfWords = Collections
            .newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    /**
     * Populates in-memory needs for text processing.
     */

    static {
        try {
            populateBannedSetOfWords();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * TEXT-CENSORING : Reads the files that contains ROT-13 version of all
     * banned words.
     */
    private static void populateBannedSetOfWords() throws IOException, FileNotFoundException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(TextCensor.class.getResourceAsStream(BANNED_LIST_FILE_NAME)))) {
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

    /**
     * Censors the word. Example : CENSOR becomes C****R.
     */
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
     * Finds banned words.
     */
    public static String censorBannedWords(String content) {
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
