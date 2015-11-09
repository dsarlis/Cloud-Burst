package org.cloudburst.etl.util;

import java.util.List;
import java.util.Map;

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
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static <K, V> String join(Map<K, V> map, String innerSeparator, String outerSeparator) {
        StringBuilder keysBuilder = new StringBuilder();
        StringBuilder valuesBuilder = new StringBuilder();
        boolean first = true;

        for (Map.Entry<K, V> element : map.entrySet()) {
            if (first) {
                first = false;
            } else {
                keysBuilder.append(innerSeparator);
                valuesBuilder.append(innerSeparator);
            }
            keysBuilder.append(element.getKey());
            valuesBuilder.append(element.getValue());
        }
        keysBuilder.append(outerSeparator).append(valuesBuilder);
        return keysBuilder.toString();
    }

    public static <T> String joinArray(List<T> list, String separator) {
        StringBuilder result = new StringBuilder();
        boolean first = true;

        for (T element: list) {
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
