package testing;

import client.KVStoreConnection;
import ecs.ECS;
import ecs.ECSNode;
import ecs.HashRing;
import org.apache.log4j.Logger;
import org.junit.*;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage;

import java.util.ArrayList;

import static shared.messages.KVMessage.StatusType.SERVER_STOPPED;

public class ECSTest extends Assert {
    private static final Logger logger = Logger.getLogger("ECSTest");

    private static ECS ecs;
    private static String zkHost = "127.0.0.1";
    private static int zkPort = 2181;
    private static int baseKVServerPort = 9000;

    private static int serverCounter = -1;

    private void addNodes(int nodes){
        ecs.addNodes(nodes);
        serverCounter += nodes;
    }

    private JsonKVMessage getMessage(KVStoreConnection kvClient, String key) throws Exception {
        JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.GET);
        req.setKey(key);
        JsonKVMessage res;

        kvClient.sendMessage(req);
        return kvClient.receiveMessage();
    }

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
        addNodes(1);

        KVStoreConnection kvClient = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient.connect();
    }

    @Test()
    public void testECSStop() throws Exception{
        addNodes(1);
        KVStoreConnection kvClient = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
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

    @Test()
    public void testECSMetaDataUpdate() throws Exception{
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + String.valueOf(serverCounter +1);
        KVMessage message = kvClient0.put(key, "bar");
        JsonKVMessage res = getMessage(kvClient0, key);
        assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

        // The second added server is called server4 so it should be responsible for its own key
        addNodes(1);

        res = getMessage(kvClient0, key);
        assert(res.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
    }

    @Test()
    public void testMoveData() throws Exception{
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + String.valueOf(serverCounter +1);
        KVMessage message = kvClient0.put(key, "bar");
        JsonKVMessage res = getMessage(kvClient0, key);
        assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

        // The second added server is called server1 so it should be responsible for its own key
        addNodes(1);
        KVStoreConnection kvClient1 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient1.connect();

        res = getMessage(kvClient1, key);
        assert(res.getValue().equals("bar"));
    }

    @Test()
    public void testRemoveNode() throws Exception{
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + String.valueOf(serverCounter + 1);;
        KVMessage message = kvClient0.put(key, "bar");
        JsonKVMessage res = getMessage(kvClient0, key);
        assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

        // The second added server is called server7 so it should be responsible for its own key
        addNodes(1);
        ecs.removeNode("server" + String.valueOf(serverCounter));

        res = getMessage(kvClient0, key);
        assert(res.getValue().equals("bar"));
    }

    @Test()
    public void testAddMultipleNodes() throws Exception{
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        for (int i = 0; i <= 2; i++){
            String key = "server" + String.valueOf(serverCounter +i);
            KVMessage message = kvClient0.put(key, "bar");
            JsonKVMessage res = getMessage(kvClient0, key);
            assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        addNodes(2);

        for (int i = 2; i >= 0; i--){
            // Current serverNumber has been incremented by 2 from first server added
            int port = baseKVServerPort + serverCounter - i;
            KVStoreConnection kvClient = new KVStoreConnection("localhost", port);
            kvClient.connect();
            String key = "server" + String.valueOf(serverCounter - i);
            System.out.println("PORT: " + String.valueOf(port) + " | KEY: " + key);
            JsonKVMessage res = getMessage(kvClient, key);
            assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

    }

    @Test()
    public void testRemoveMultipleNodes () throws Exception{
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        for (int i = 0; i <= 2; i++){
            String key = "server" + String.valueOf(serverCounter +i);
            KVMessage message = kvClient0.put(key, "bar");
            JsonKVMessage res = getMessage(kvClient0, key);
            assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        addNodes(2);

        ArrayList<String> nodesToRemove = new ArrayList<String>();
        nodesToRemove.add("server" + String.valueOf(serverCounter - 2));
        nodesToRemove.add("server" + String.valueOf(serverCounter - 1));
        ecs.removeNodes(nodesToRemove);

        KVStoreConnection kvClient2 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient2.connect();
        for (int i = 2; i >= 0; i--){
            String key = "server" + String.valueOf(serverCounter +i);
            JsonKVMessage res = getMessage(kvClient2, key);
            assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

    }

}
