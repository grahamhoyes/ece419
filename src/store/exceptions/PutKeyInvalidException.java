package store.exceptions;

public class PutKeyInvalidException extends Exception{
    public PutKeyInvalidException(String key) {
        super("Invalid key used for deletion: " + key);
    }
}
