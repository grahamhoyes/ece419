package ecs;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class HashRing {
    protected ArrayList<ServerNode> serverNodes = new ArrayList<>();

    public HashRing() {}

    public HashRing(ServerNode[] nodes) {
        Collections.addAll(serverNodes, nodes);
        Collections.sort(serverNodes);

        // Link the nodes together
        ServerNode lastNode = serverNodes.get(serverNodes.size() - 1);
        serverNodes.get(0).setPredecessor(lastNode.getNodeHash());

        for (int i = 1; i < serverNodes.size(); i++) {
            ServerNode prev = serverNodes.get(i-1);
            ServerNode cur = serverNodes.get(i);
            cur.setPredecessor(prev.getNodeHash());
        }
    }

    public HashRing(String json) {
        deserialize(json);
    }

    public Collection<ServerNode> getNodes() {
        return serverNodes;
    }

    public ServerNode getNode(int index) {
        return serverNodes.get(index);
    }

    public ServerNode getNode(String nodeName) {
        for (ServerNode node : serverNodes) {
            if (node.getNodeName().equals(nodeName)) {
                return node;
            }
        }

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

        node.setPredecessor(predecessor.getNodeHash());
        successor.setPredecessor(node.getNodeHash());
    }

    public ServerNode removeNode(int index) {
        ServerNode node = serverNodes.get(index);

        int predecessorIdx = index == 0 ? serverNodes.size() - 1 : index - 1;
        int successorIdx = index == serverNodes.size() - 1 ? 0 : index + 1;

        ServerNode predecessor = serverNodes.get(predecessorIdx);
        ServerNode successor = serverNodes.get(successorIdx);

        serverNodes.remove(index);
        successor.setPredecessor(predecessor.getNodeHash());

        return node;
    }

    public ServerNode removeNode(String nodeName) {
        int idx = -1;

        for (int i = 0; i < serverNodes.size(); i++) {
            if (serverNodes.get(i).getNodeName().equals(nodeName)) {
                idx = i;
                break;
            }
        }

        return removeNode(idx);
    }

    public ServerNode getNodeForKey(String key) {
        for (ServerNode node : serverNodes) {
            if (node.isNodeResponsible(key)) {
                return node;
            }
        }

        return null;
    }

    public ServerNode getSuccessor(ServerNode node) {
        for (ServerNode other : serverNodes) {
            if (other.getPredecessorHash().equals(node.getNodeHash()))
                return other;
        }

        return null;
    }

    public String serialize() {
        return new Gson().toJson(this);
    }

    public void deserialize(String json) {
        HashRing hashRingFromJson = new Gson().fromJson(json, HashRing.class);
        this.serverNodes = hashRingFromJson.serverNodes;
        Collections.sort(serverNodes);
    }

    /**
     * @return A deep copy of this HashRing
     */
    public HashRing copy() {
        ArrayList<ServerNode> copyNodes = new ArrayList<>();

        for (ServerNode node : this.serverNodes) {
            copyNodes.add(node.copy());
        }

        HashRing copyHashRing = new HashRing();
        copyHashRing.serverNodes = copyNodes;

        return copyHashRing;
    }
}
