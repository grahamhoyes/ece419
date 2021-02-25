package testing;

import client.KVStoreConnection;
import ecs.ECS;
import ecs.ECSNode;
import ecs.HashRing;
import org.apache.log4j.Logger;
import org.junit.*;
import shared.messages.KVMessage;

import java.util.Map;

import static shared.messages.KVMessage.StatusType.SERVER_STOPPED;

public class ECSTest extends Assert {
    private static final Logger logger = Logger.getLogger("ECSTest");

    private static ECS ecs;
    private static String zkHost = "127.0.0.1";
    private static int zkPort = 2181;

    @BeforeClass
    public static void setUp() {
        ecs = new ECS("ecs_test.config", zkHost, zkPort, System.getenv("KV_SERVER_JAR"));
    }

    @After
    public void shutDownNodes() {
        ecs.shutdown();
    }

    @AfterClass
    public static void tearDown() {
        ecs.shutdown();
    }

    @Test()
    public void testSingleServerStartup() throws Exception{
        ecs.addNodes(1);

        KVStoreConnection kvClient = new KVStoreConnection("localhost", 9000);
        kvClient.connect();
    }

    @Test()
    public void testECSStop() throws Exception{
        ecs.addNodes(1);

        KVStoreConnection kvClient = new KVStoreConnection("localhost", 9001);

        kvClient.connect();

        ecs.stop();

        KVMessage message = kvClient.put("foo", "bar");
        logger.info("Received Message: " + message.getStatus());
        assert(message.getStatus() == SERVER_STOPPED);
    }

    @Test()
    public void testSingleNodeHashRing() throws Exception{
        ECSNode node0 = new ECSNode("server0", "127.0.0.1", 9000);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);

        assert (node0.getPredecessorHash().equals(node0.getNodeHash()));
    }

    @Test()
    public void testUpdateHashRing(){
        ECSNode node0 = new ECSNode("server0", "127.0.0.1", 9000);
        ECSNode node1 = new ECSNode("server1", "127.0.0.1", 9001);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);

        assert (node0.getPredecessorHash().equals(node1.getNodeHash()));
        assert (node1.getPredecessorHash().equals(node0.getNodeHash()));
    }

    @Test()
    public void testRemoveFromHashRing(){
        ECSNode node0 = new ECSNode("server0", "127.0.0.1", 9000);
        ECSNode node1 = new ECSNode("server1", "127.0.0.1", 9001);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);

        hashRing.removeNode("server1");

        assert (node0.getPredecessorHash().equals(node0.getNodeHash()));
    }

    @Test()
    public void testHashing(){
        ECSNode node0 = new ECSNode("server0", "127.0.0.1", 9000);
        ECSNode node1 = new ECSNode("server1", "127.0.0.1", 9001);

        HashRing hashRing = new HashRing();
        hashRing.addNode(node0);
        hashRing.addNode(node1);

        assert (node0.isNodeResponsible("server0"));
        assert (!node0.isNodeResponsible("server1"));
    }

}
