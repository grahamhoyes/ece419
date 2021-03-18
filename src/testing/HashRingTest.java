package testing;

import ecs.ServerNode;
import ecs.HashRing;
import org.junit.*;

public class HashRingTest extends Assert {

    @Test()
    public void testHashing() {
        ServerNode node0 = new ServerNode("server0", "127.0.0.1", 9000);
        ServerNode node1 = new ServerNode("server1", "127.0.0.1", 9001);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);

        assert (node0.isNodeResponsible("server0"));
        assert (!node0.isNodeResponsible("server1"));

        assert (node1.isNodeResponsible("server1"));
        assert (!node1.isNodeResponsible("server0"));
    }

    @Test()
    public void testSingleNodeHashRing() {
        ServerNode node0 = new ServerNode("server0", "127.0.0.1", 9000);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);

        assert (node0.getPredecessor().equals(node0));
    }

    @Test()
    public void testUpdateHashRing() {
        ServerNode node0 = new ServerNode("server0", "127.0.0.1", 9000);
        ServerNode node1 = new ServerNode("server1", "127.0.0.1", 9001);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);

        assert (node0.getPredecessor().equals(node1));
        assert (node1.getPredecessor().equals(node0));
    }

    @Test()
    public void testRemoveFromHashRing() {
        ServerNode node0 = new ServerNode("server0", "127.0.0.1", 9000);
        ServerNode node1 = new ServerNode("server1", "127.0.0.1", 9001);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);

        hashRing.removeNode("server1");

        assert (node0.getPredecessor().getNodeHash().equals(node0.getNodeHash()));
    }

    @Test()
    public void testDoesNodeReplicate() {
        /*
        * [('server3', '0ddacf356d025a8e6068b42ac537127e'),
        * ('server2', '194f9987498c1cf5a795d83caa147814'),
        * ('server1', 'a8438da78e679f44a5cff9e44ebacfbd'),
        * ('server0', 'c8e88753b97c508edf4dd3eb4892d275')]
        * */
        ServerNode node0 = new ServerNode("server0", "127.0.0.1", 9000);
        ServerNode node1 = new ServerNode("server1", "127.0.0.1", 9001);
        ServerNode node2 = new ServerNode("server2", "127.0.0.1", 9002);
        ServerNode node3 = new ServerNode("server3", "127.0.0.1", 9003);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);
        hashRing.addNode(node2);
        hashRing.addNode(node3);

        assert (node0.doesNodeReplicateKey("server1"));
        assert (node0.doesNodeReplicateKey("server2"));
        assert (!node0.doesNodeReplicateKey("server3"));
        assert (!node0.doesNodeReplicateKey("server0"));
    }

    @Test
    public void testServerNodeEquality() {
        ServerNode node = new ServerNode("server0", "127.0.0.1", 9000);
        ServerNode nodeAgain = new ServerNode("server0", "127.0.0.1", 9000);
        ServerNode notNode = new ServerNode("server1", "127.0.0.1", 9001);

        assert (node.equals(nodeAgain));
        assert (!node.equals(notNode));
    }

    @Test
    public void testSerialization() {
        ServerNode node0 = new ServerNode("server0", "127.0.0.1", 9000);
        ServerNode node1 = new ServerNode("server1", "127.0.0.1", 9001);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);

        hashRing.serialize();
    }

    @Test
    public void testDeserialization() {
        ServerNode node0 = new ServerNode("server0", "127.0.0.1", 9000);
        ServerNode node1 = new ServerNode("server1", "127.0.0.1", 9001);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);

        String json = hashRing.serialize();

        HashRing hashRingFromJson = new HashRing(json);

        assert (hashRingFromJson.getNodes().size() == 2);

        ServerNode node0Deserialized = hashRingFromJson.getNode("server0");
        assert (node0Deserialized.equals(node0));

        ServerNode node1Deserialized = node0Deserialized.getSuccessor();
        assert (node1Deserialized.equals(node1));

        assert (node1Deserialized.getSuccessor().equals(node0Deserialized));
    }
}
