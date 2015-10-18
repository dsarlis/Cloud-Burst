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
        StringBuilder[] builders = new StringBuilder[n];

        for (int index = 0, j = 0, slice = 0; index < message.length(); index++) {
            int letterValue = INVERSE_MAP.get(message.charAt(index));
            int letterValueWithoutOffset = letterValue - offset;
            if (letterValueWithoutOffset < 0) {
                letterValueWithoutOffset = ABC.length + letterValueWithoutOffset;
            }

            if (builders[j] == null) {
                builders[j] = new StringBuilder();
            }
            builders[j].append(ABC[letterValueWithoutOffset]);
            j++;
            if (j > slice || j == n) {
                slice++;
                j = slice < n ? 0 :  (slice % n) + 1;
            }
        }

        StringBuilder result = new StringBuilder();

        for (StringBuilder builder : builders) {
            result.append(builder);
        }

        return result.toString();
    }

    public static void main(String[] args) {
        CipherService service = new CipherService();

        System.out.println(service.decrypt("306063896731552281713201727176392168770237379582172677299123272033941091616817696059536783089054693601", "URYYBBJEX"));
    }

}
