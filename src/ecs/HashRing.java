package ecs;

import com.google.gson.Gson;

import java.util.ArrayList;
import java.util.Collections;

public class HashRing {
    protected ArrayList<ECSNode> ecsNodes = new ArrayList<>();

    public HashRing() {}

    public HashRing(ECSNode[] nodes) {
        Collections.addAll(ecsNodes, nodes);
        Collections.sort(ecsNodes);
    }

    public HashRing(String json) {
        deserialize(json);
    }

    public ECSNode getNode(int index) {
        return ecsNodes.get(index);
    }

    public void addNode(ECSNode node) {
        // Insert into the array list
        ecsNodes.add(node);

        // Not super efficient, but oh well
        Collections.sort(ecsNodes);

        // Find the predecessor and successor nodes
        int index = ecsNodes.indexOf(node);

        int predecessorIdx = index == 0 ? ecsNodes.size() - 1 : index - 1;
        int successorIdx = index == ecsNodes.size() - 1 ? 0 : index + 1;

        ECSNode predecessor = ecsNodes.get(predecessorIdx);
        ECSNode successor = ecsNodes.get(successorIdx);

        node.setPredecessor(predecessor.getNodeHash());
        successor.setPredecessor(successor.getNodeHash());
    }

    ECSNode removeNode(int index) {
        ECSNode node = ecsNodes.get(index);

        int predecessorIdx = index == 0 ? ecsNodes.size() - 1 : index - 1;
        int successorIdx = index == ecsNodes.size() - 1 ? 0 : index + 1;

        ECSNode predecessor = ecsNodes.get(predecessorIdx);
        ECSNode successor = ecsNodes.get(successorIdx);

        ecsNodes.remove(index);
        successor.setPredecessor(predecessor.getNodeHash());

        return node;
    }

    public String serialize() {
        return new Gson().toJson(this);
    }

    public void deserialize(String json) {
        HashRing hashRingFromJson = new Gson().fromJson(json, HashRing.class);
        this.ecsNodes = hashRingFromJson.ecsNodes;
        Collections.sort(ecsNodes);
    }
}
