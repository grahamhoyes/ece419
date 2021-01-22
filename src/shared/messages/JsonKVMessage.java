package shared.messages;

import com.google.gson.Gson;

public class JsonKVMessage implements KVMessage {
    private StatusType status;
    private String key;
    private String value;

    /**
     * Generate a JSON KV message from the provided status, key, and value
     *
     * @param status Message status
     * @param key Message key
     * @param value Message value
     */
    public JsonKVMessage(StatusType status, String key, String value) {
        this.status = status;
        this.key = key;
        this.value = value;
    }

    /**
     * Generate a JSON KV message from a JSON encoded message
     *
     * @param json JSON string of the message object
     */
    public JsonKVMessage(String json) {
        deserialize(json);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public StatusType getStatus() {
        return status;
    }

    public String serialize() {
        return new Gson().toJson(this);
    }

    public void deserialize(String json) {
        JsonKVMessage message = new Gson().fromJson(json, JsonKVMessage.class);
        this.status = message.status;
        this.key = message.key;
        this.value = message.value;
    }
}
