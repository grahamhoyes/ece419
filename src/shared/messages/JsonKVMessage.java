package shared.messages;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import ecs.HashRing;

public class JsonKVMessage implements KVMessage, Serializable {
    private StatusType status;
    private String key;
    private String value;
    private Long ttl;
    private String message;
    private HashRing metadata;

    /**
     * Create a new JSON KV message
     *
     * Attributes to be set via setters
     */
    public JsonKVMessage() {

    }

    /**
     * Generate a JSON KV message from the provided status
     *
     * @param status Message status
     */
    public JsonKVMessage(StatusType status) {
        this.status = status;
    }

    /**
     * Generate a JSON KV message from a JSON encoded message
     *
     * @param json JSON string of the message object
     */
    public JsonKVMessage(String json) throws DeserializationException {
        deserialize(json);
    }

    public StatusType getStatus() {
        return status;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getMessage() {
        return message;
    }

    public HashRing getMetadata() {
        return metadata;
    }

    public Long getTTL() {
        return ttl;
    }

    public void setStatus(StatusType status) {
        this.status = status;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setMetadata(HashRing hashRing) {
        this.metadata = hashRing;
    }

    public void setTTL(Long ttl) {
        this.ttl = ttl;
    }

    public String serialize() {
        return new Gson().toJson(this);
    }

    public void deserialize(String json) throws DeserializationException {
        try {
            JsonKVMessage kvMessage = new Gson().fromJson(json, JsonKVMessage.class);
            this.status = kvMessage.status;

            if (this.status == null)
                throw new DeserializationException("Failed to deserialize message: " + json);

            this.key = kvMessage.key;
            this.value = kvMessage.value;
            this.ttl = kvMessage.ttl;
            this.message = kvMessage.message;
            this.metadata = kvMessage.metadata;
            if (this.metadata != null) {
                this.metadata.rebuildHashRingLinkedList();
            }
        } catch (JsonSyntaxException e) {
            throw new DeserializationException("Failed to deserialize message: " + json);
        }

    }
}
