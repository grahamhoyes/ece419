package store;

public class KeyExpiredException extends KeyInvalidException {
    public KeyExpiredException(String key) {
        super(key);
    }
}
