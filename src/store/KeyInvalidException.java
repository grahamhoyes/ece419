package store;

public class KeyInvalidException extends Exception {
    public KeyInvalidException(String key) {
        super("Invalid key used: " + key);
    }
}
