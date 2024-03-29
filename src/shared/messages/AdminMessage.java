package shared.messages;

import com.google.gson.Gson;
import ecs.ServerNode;
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

    public enum ServerChange {
        ADDED,
        DELETED,
        DIED,
        STARTED,  // When this happens, changedServer will be null
        STOPPED,  // When this happens, changedServer will be null
        SETTINGS, // When this happens, changedServer will be null
    }

    private Action action;              // Required for all messages
    private String uuid;                // Required for all messages
    private String message;             // Only required for errors
    private HashRing hashRing;          // Used by SET_METADATA, MOVE_DATA
    private ServerChange serverChange;  // Used by SET_METADATA
    private ServerNode changedServer;   // Used by SET_METADATA

    /* Required for move/receive messages
     * The sender and receiver nodes have no guarantee about having
     * their hash ranges correct, trust the range field */
    private ServerNode sender;
    private ServerNode receiver;


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

    public ServerNode getSender() {
        return sender;
    }

    public ServerNode getReceiver() {
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

    public void setSender(ServerNode sender) {
        this.sender = sender;
    }

    public void setReceiver(ServerNode receiver) {
        this.receiver = receiver;
    }

    public ServerChange getServerChange() {
        return serverChange;
    }

    public void setServerChange(ServerChange serverChange) {
        this.serverChange = serverChange;
    }

    public ServerNode getChangedServer() {
        return changedServer;
    }

    public void setChangedServer(ServerNode changedServer) {
        this.changedServer = changedServer;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String serialize() {
        return new Gson().toJson(this);
    }

    public void deserialize(String json) {
        AdminMessage adminMessage = new Gson().fromJson(json, AdminMessage.class);

        this.action = adminMessage.action;
        this.uuid = adminMessage.uuid;
        this.message = adminMessage.message;
        this.hashRing = adminMessage.hashRing;
        if (this.hashRing != null) {
            this.hashRing.rebuildHashRingLinkedList();
        }
        this.sender = adminMessage.sender;
        this.receiver = adminMessage.receiver;
        this.serverChange = adminMessage.serverChange;
        this.changedServer = adminMessage.changedServer;
    }
}