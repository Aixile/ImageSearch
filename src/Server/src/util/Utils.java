package util;

import java.io.IOException;

public class Utils {
    public static boolean endsWith(byte[] byteArray, int length, String endString) throws IOException {
        if (length < endString.length()) return false;
        return new String(byteArray, length - endString.length(), endString.length(), "UTF-8").equals(endString);
    }
    
}
