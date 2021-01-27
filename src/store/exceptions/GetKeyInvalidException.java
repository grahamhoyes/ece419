package store.exceptions;

public class GetKeyInvalidException extends Exception{
    public GetKeyInvalidException(String key) {
        super("Invalid key used: " + key);
    }
}
