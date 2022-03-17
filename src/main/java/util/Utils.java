package util;

public class Utils {
    public static boolean isValidIdentity(String identity){
        return Character.toString(identity.charAt(0)).matches("[a-zA-Z]+") && identity.matches("[a-zA-Z0-9]+") && identity.length() >= 3 && identity.length() <= 16;
    }
}
