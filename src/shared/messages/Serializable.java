package shared.messages;

public interface Serializable {

    String serialize();
    void deserialize(String data) throws DeserializationException;

}
