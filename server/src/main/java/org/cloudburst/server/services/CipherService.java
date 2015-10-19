package org.cloudburst.server.services;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CipherService {

    private static final BigInteger X = new BigInteger("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");
    private static final char[] ABC = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
    private static Map<Character, Integer> INVERSE_MAP = new ConcurrentHashMap<Character, Integer>();

    static {
        INVERSE_MAP.put('A', 0);
        INVERSE_MAP.put('B', 1);
        INVERSE_MAP.put('C', 2);
        INVERSE_MAP.put('D', 3);
        INVERSE_MAP.put('E', 4);
        INVERSE_MAP.put('F', 5);
        INVERSE_MAP.put('G', 6);
        INVERSE_MAP.put('H', 7);
        INVERSE_MAP.put('I', 8);
        INVERSE_MAP.put('J', 9);
        INVERSE_MAP.put('K', 10);
        INVERSE_MAP.put('L', 11);
        INVERSE_MAP.put('M', 12);
        INVERSE_MAP.put('N', 13);
        INVERSE_MAP.put('O', 14);
        INVERSE_MAP.put('P', 15);
        INVERSE_MAP.put('Q', 16);
        INVERSE_MAP.put('R', 17);
        INVERSE_MAP.put('S', 18);
        INVERSE_MAP.put('T', 19);
        INVERSE_MAP.put('U', 20);
        INVERSE_MAP.put('V', 21);
        INVERSE_MAP.put('W', 22);
        INVERSE_MAP.put('X', 23);
        INVERSE_MAP.put('Y', 24);
        INVERSE_MAP.put('Z', 25);
    }

    public String decrypt(String key, String message) {
        BigInteger xy = new BigInteger(key);
        BigInteger y = xy.divide(X);
        int offset = (y.intValue() % 25) + 1;
        int n = (int) Math.sqrt(message.length());
        StringBuilder result = new StringBuilder();
        char[][] matrix = new char[n][n];
        int index = 0;

        for (int row = 0; row < n; row++) {
            for (int col = 0; col< n; col++) {
                matrix[row][col] = message.charAt(index++);
            }
        }

        for (int slice = 0; slice < 2 * n - 1; ++slice) {
            int z = slice < n ? 0 : slice - n + 1;

            for (int j = z; j <= slice - z; ++j) {
                int letterValue = INVERSE_MAP.get(matrix[j][slice - j]);
                int letterValueWithoutOffset = letterValue - offset;

                if (letterValueWithoutOffset < 0) {
                    letterValueWithoutOffset = ABC.length + letterValueWithoutOffset;
                }
                result.append(ABC[letterValueWithoutOffset]);
            }
        }

        return result.toString();
    }

}
