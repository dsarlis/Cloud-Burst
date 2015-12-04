package org.cloudburst.etl.util;

import java.util.List;

/**
 * Util class for string.
 */
public class StringUtil {

    private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

    /**
     * Translates by array into String.
     */
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    /**
     * Transforms list into a string. It joins the elements of the list with the
     * given separator.
     */
    public static <T> String joinArray(List<T> list, String separator) {
        StringBuilder result = new StringBuilder();
        boolean first = true;

        for (T element : list) {
            if (first) {
                first = false;
            } else {
                result.append(separator);
            }
            result.append(element);
        }
        return result.toString();
    }

}
