package shared.messages;

import com.google.gson.Gson;
import ecs.ECSNode;
import ecs.HashRing;

public class AdminMessage implements Serializable {

    public enum Action {
        NOP,              // Do nothing
        INIT,             // Initialize the KVServer
        START,            // Start accepting requests
        STOP,             // Stop accepting requests
        SHUT_DOWN,        // Shut down the KVServer
        WRITE_LOCK,       // Lock the KVServer for write requests
        WRITE_UNLOCK,     // Unlock the KVServer for write requests
        MOVE_DATA,        // Move a subset of data to another KVServer
        RECEIVE_DATA,     // Receive data from another KVServer
        CLEANUP_DATA,
        SET_METADATA,     // Sets the metadata for a particular KVServer. Broadcast metadata
                          // updates are done by updating the metadata ZNode afterwards
        ACK,              // Acknowledge success
        ERROR,            // Action failed
    }

    private Action action;         // Required for all messages
    private String message;        // Only required for errors
    private HashRing hashRing;     // Used by UPDATE_METADATA

    /* Required for move/receive messages
     * The sender and receiver nodes have no guarantee about having
     * their hash ranges correct, trust the range field */
    private ECSNode sender;
    private ECSNode receiver;

    public AdminMessage() {

    }

    public AdminMessage(Action action) {
        this.action = action;
    }

    public AdminMessage(String json) {
        deserialize(json);
    }

    public Action getAction() {
        return action;
    }

    public String getMessage() {
        return message;
    }

    public ECSNode getSender() {
        return sender;
    }

    public ECSNode getReceiver() {
        return receiver;
    }

    public HashRing getMetadata() {
        return this.hashRing;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setMetadata(HashRing hashRing) {
        this.hashRing = hashRing;
    }

    public void setSender(ECSNode sender) {
        this.sender = sender;
    }

    public void setReceiver(ECSNode receiver) {
        this.receiver = receiver;
    }

    public String serialize() {
        return new Gson().toJson(this);
    }

    public void deserialize(String json) {
        AdminMessage adminMessage = new Gson().fromJson(json, AdminMessage.class);

        this.action = adminMessage.action;
        this.message = adminMessage.message;
        this.hashRing = adminMessage.hashRing;
        this.sender = adminMessage.sender;
        this.receiver = adminMessage.receiver;
    }
}
