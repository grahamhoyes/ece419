package shared.messages;

public class DeserializationException extends Exception {
    public DeserializationException(String message) {
        super("Failed to deserialize message: " + message);
    }
}
