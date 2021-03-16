package ecs;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Class for tracking the Hash Ring used for persistent storage
 * <p>
 * Nodes are stored in two places: In a sorted ArrayList,
 * and as an actual linked list. The ArrayList's only purpose
 * is to support serialization and deserialization.
 */
public class HashRing {
    private static final Logger logger = Logger.getLogger("HashRing");
    private ArrayList<ServerNode> serverNodes = new ArrayList<>();

    public HashRing() {
    }

    public HashRing(String json) {
        deserialize(json);
    }

    /**
     * Link the nodes in the provided ArrayList into a doubly
     * linked list themselves
     *
     * @param nodes Sorted ArrayList of ServerNodes
     */
    private static void rebuildNodeLinkedList(ArrayList<ServerNode> nodes) {
        ServerNode start = null;

        for (int i = 0; i < nodes.size(); i++) {
            ServerNode node = nodes.get(i);

            if (i == 0) {
                start = node;
                node.setPredecessor(node);
                node.setSuccessor(node);
            } else {
                ServerNode lastNode = start.getPredecessor();
                lastNode.setSuccessor(node);
                node.setPredecessor(lastNode);
                node.setSuccessor(start);
                start.setPredecessor(node);
            }
        }
    }

    public Collection<ServerNode> getNodes() {
        return serverNodes;
    }

    public ServerNode getNode(String nodeName) {
        for (ServerNode node : serverNodes) {
            if (node.getNodeName().equals(nodeName)) {
                return node;
            }
        }

        logger.error("Node " + nodeName + " not found");

        return null;
    }

    public void addNode(ServerNode node) {
        // Insert into the array list
        serverNodes.add(node);

        // Not super efficient, but oh well
        Collections.sort(serverNodes);

        // Find the predecessor and successor nodes
        int index = serverNodes.indexOf(node);

        int predecessorIdx = index == 0 ? serverNodes.size() - 1 : index - 1;
        int successorIdx = index == serverNodes.size() - 1 ? 0 : index + 1;

        ServerNode predecessor = serverNodes.get(predecessorIdx);
        ServerNode successor = serverNodes.get(successorIdx);

        // Insert into the linked list
        node.setPredecessor(predecessor);
        node.setSuccessor(successor);
        predecessor.setSuccessor(node);
        successor.setPredecessor(node);
    }

    public void removeNode(int index) {
        if (index < 0 || index >= serverNodes.size()) {
            logger.error("Cannot remove node at index " + index + ". Out of range.");
        };

        ServerNode node = serverNodes.get(index);
        serverNodes.remove(index);

        ServerNode predecessor = node.getPredecessor();
        ServerNode successor = node.getSuccessor();

        predecessor.setSuccessor(successor);
        successor.setPredecessor(predecessor);

    }

    public void removeNode(String nodeName) {
        int idx = -1;

        for (int i = 0; i < serverNodes.size(); i++) {
            if (serverNodes.get(i).getNodeName().equals(nodeName)) {
                idx = i;
                break;
            }
        }

        if (idx < 0) {
            logger.error("Cannot remove node " + nodeName + ". Node not found.");
        }

        removeNode(idx);
    }

    public ServerNode getNodeForKey(String key) {
        for (ServerNode node : serverNodes) {
            if (node.isNodeResponsible(key)) {
                return node;
            }
        }

        return null;
    }

    public String serialize() {
        return new Gson().toJson(this);
    }

    public void rebuildHashRingLinkedList() {
        rebuildNodeLinkedList(this.serverNodes);
    }

    public void deserialize(String json) {
        HashRing hashRingFromJson = new Gson().fromJson(json, HashRing.class);
        this.serverNodes = hashRingFromJson.serverNodes;
        Collections.sort(serverNodes);

        // The deserialized nodes won't have their successors and
        // predecessors set, so fix that
        rebuildHashRingLinkedList();
    }

    /**
     * @return A deep copy of this HashRing
     */
    public HashRing copy() {
        ArrayList<ServerNode> copyNodes = new ArrayList<>();

        for (ServerNode node : this.serverNodes) {
            copyNodes.add(node.copy());
        }

        rebuildNodeLinkedList(copyNodes);

        HashRing copyHashRing = new HashRing();
        copyHashRing.serverNodes = copyNodes;

        return copyHashRing;
    }
}
