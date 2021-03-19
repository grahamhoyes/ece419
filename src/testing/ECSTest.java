package testing;

import app_kvServer.KVServer;
import client.KVStoreConnection;
import ecs.ECS;
import ecs.ServerNode;
import ecs.HashRing;
import org.apache.log4j.Logger;
import org.junit.*;
import shared.messages.JsonKVMessage;
import shared.messages.KVMessage;
import store.KVStore;

import java.util.ArrayList;

import static shared.messages.KVMessage.StatusType.SERVER_STOPPED;
import static shared.messages.KVMessage.StatusType.valueOf;

public class ECSTest extends Assert {
    private static final Logger logger = Logger.getLogger("ECSTest");

    private static ECS ecs;
    private static final String zkHost = "127.0.0.1";
    private static final int zkPort = 2181;
    private static final int baseKVServerPort = 9000;

    private static int serverCounter = -1;

    private void addNodes(int nodes) {
        ecs.addNodes(nodes);
        serverCounter += nodes;
    }

    private JsonKVMessage getMessage(KVStoreConnection kvClient, String key) throws Exception {
        JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.GET);
        req.setKey(key);

        kvClient.sendMessage(req);
        return kvClient.receiveMessage();
    }

    private JsonKVMessage putMessage(KVStoreConnection kvClient, String key, String value) throws Exception {
        JsonKVMessage req = new JsonKVMessage(KVMessage.StatusType.PUT);
        req.setKey(key);
        req.setValue(value);

        kvClient.sendMessage(req);
        return kvClient.receiveMessage();
    }

    @BeforeClass
    public static void setUp()  {
        ecs = new ECS("ecs_test.config", zkHost, zkPort, System.getenv("KV_SERVER_JAR"));
    }

    @After
    public void shutDownNodes()  {
        ecs.shutdown();
    }


    @Test()
    public void testSingleServerStartup() throws Exception {
        addNodes(1);

        KVStoreConnection kvClient = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient.connect();
    }

    @Test()
    public void testECSStop() throws Exception {
        addNodes(1);
        KVStoreConnection kvClient = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient.connect();

        ecs.stop();

        KVMessage message = kvClient.put("foo", "bar");
        logger.info("Received Message: " + message.getStatus());
        assert(message.getStatus() == SERVER_STOPPED);
    }

    @Test()
    public void testECSMetaDataUpdate() throws Exception {
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + (serverCounter + 1);
        KVMessage message = kvClient0.put(key, "bar");
        JsonKVMessage res = getMessage(kvClient0, key);
        assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

        // The second added server is called server4 so it should be responsible for its own key
        addNodes(1);

//        res = getMessage(kvClient0, key);
        res = putMessage(kvClient0, key, "bar1");
        assert(res.getStatus() == KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
    }

    @Test()
    public void testReplicatorGet() throws Exception {
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + (serverCounter + 1);
        KVMessage message = kvClient0.put(key, "bar");
        JsonKVMessage res = getMessage(kvClient0, key);
        assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

        // The second added server is called server4 so it should be responsible for its own key
        addNodes(1);

        res = getMessage(kvClient0, key);
        assert(res.getStatus() == KVMessage.StatusType.GET_SUCCESS);
    }

    @Test()
    public void testNewServerWriteLockRelease() throws Exception {
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + (serverCounter + 1);
        KVMessage message = kvClient0.put(key, "bar");
        JsonKVMessage res = getMessage(kvClient0, key);
        assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

        // The second added server is called server1 so it should be responsible for its own key
        addNodes(1);
        KVStoreConnection kvClient1 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient1.connect();

        message = kvClient1.put("foo", "bar");
        assert(message.getStatus() == KVMessage.StatusType.PUT_SUCCESS);
        message = kvClient1.put(key, "bar1");
        assert(message.getStatus() == KVMessage.StatusType.PUT_UPDATE);

    }

    @Test()
    public void testMoveRelevantDataToNewServer() throws Exception {
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + (serverCounter);
        KVMessage message = kvClient0.put(key, "bar");
        JsonKVMessage res = getMessage(kvClient0, key);
        assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

        // The second added server is called server1 so it should be responsible for its own key
        addNodes(1);
        KVStoreConnection kvClient1 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient1.connect();

        res = getMessage(kvClient0, key);
        assert(res.getValue().equals("bar"));
    }

    @Test()
    public void testMoveDataToNewServer() throws Exception {
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + (serverCounter + 1);
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
    public void testMoveDataAfterRemoveNode() throws Exception {
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        String key = "server" + (serverCounter + 1);
        KVMessage message = kvClient0.put(key, "bar");
        JsonKVMessage res = getMessage(kvClient0, key);
        assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);

        // The second added server is called server7 so it should be responsible for its own key
        addNodes(1);
        ecs.removeNode("server" + serverCounter);

        res = getMessage(kvClient0, key);
        assert(res.getValue().equals("bar"));
    }

    @Test()
    public void testAddMultipleNodes() throws Exception {
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        for (int i = 0; i <= 2; i++) {
            String key = "server" + (serverCounter + i);
            KVMessage message = kvClient0.put(key, "bar");
            JsonKVMessage res = getMessage(kvClient0, key);
            assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        addNodes(2);

        for (int i = 2; i >= 0; i--) {
            // Current serverNumber has been incremented by 2 from first server added
            int port = baseKVServerPort + serverCounter - i;
            KVStoreConnection kvClient = new KVStoreConnection("localhost", port);
            kvClient.connect();
            String key = "server" + (serverCounter - i);
            System.out.println("PORT: " + port + " | KEY: " + key);
            JsonKVMessage res = getMessage(kvClient, key);
            assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

    }

    @Test()
    public void testRemoveMultipleNodes () throws Exception {
        addNodes(1);

        KVStoreConnection kvClient0 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient0.connect();

        // The first server is responsible for all keys
        for (int i = 0; i <= 2; i++) {
            String key = "server" + (serverCounter + i);
            KVMessage message = kvClient0.put(key, "bar");
            JsonKVMessage res = getMessage(kvClient0, key);
            assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

        addNodes(2);

        ArrayList<String> nodesToRemove = new ArrayList<String>();
        nodesToRemove.add("server" + (serverCounter - 2));
        nodesToRemove.add("server" + (serverCounter - 1));
        ecs.removeNodes(nodesToRemove);

        KVStoreConnection kvClient2 = new KVStoreConnection("localhost", baseKVServerPort + serverCounter);
        kvClient2.connect();
        for (int i = 2; i >= 0; i--) {
            String key = "server" + (serverCounter + i);
            JsonKVMessage res = getMessage(kvClient2, key);
            assert(res.getStatus() != KVMessage.StatusType.SERVER_NOT_RESPONSIBLE);
        }

    }

}
