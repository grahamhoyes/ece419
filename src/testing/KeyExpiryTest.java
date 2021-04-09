package testing;

import client.KVStoreConnection;
import ecs.ECS;
import org.junit.*;
import shared.messages.KVMessage;

public class KeyExpiryTest extends Assert {

    private KVStoreConnection kvClient;
    private static ECS ecs;
    private static final String zkHost = "127.0.0.1";
    private static final int zkPort = 2181;

    @BeforeClass
    public static void setUp() {
        ecs = new ECS("ecs_test.config", zkHost, zkPort, System.getenv("KV_SERVER_JAR"));
        ecs.addNodes(2);
    }

    @AfterClass
    public static void shutDownNodes() {
        ecs.shutdown();
    }

    @Before
    public void testSetUp() throws Exception {
        kvClient = new KVStoreConnection("localhost", 9000);
        kvClient.connect();
    }

    @Test
    public void testPutTTLNotExpired() throws Exception {
        String key = "testPutTTL";
        String value = "foobar";

        KVMessage response = kvClient.putTTL(key, value, 300L);

        assertEquals(KVMessage.StatusType.PUT_SUCCESS, response.getStatus());

        response = kvClient.get(key);
        assertEquals(value, response.getValue());

        // Cleanup
        kvClient.put(key, null);
    }

    @Test
    public void testPutTTLDeletedByCoordinator() throws Exception {
        String key = "server0";
        String value = "a";

        kvClient.putTTL(key, value, 1L);

        Thread.sleep(2000);

        KVMessage response = kvClient.get(key);
        assertEquals(KVMessage.StatusType.GET_ERROR, response.getStatus());
    }

    @Test
    public void testPutTTLNotDeletedByReplicator() throws Exception {
        String key = "server0";
        String value = "a";

        kvClient.putTTL(key, value, 1L);

        // Switch to a replicator
        kvClient.put("server1", "b");

        Thread.sleep(2000);

        KVMessage response = kvClient.get(key);

        assertEquals(KVMessage.StatusType.GET_SUCCESS, response.getStatus());
        assertEquals(value, response.getValue());
    }

    @Test
    public void testCoordinatorPropagatesTTLDelete() throws Exception {
        String key = "server0";
        String value = "a";

        kvClient.putTTL(key, value, 1L);

        Thread.sleep(2000);

        // Fetching the key from the coordinator after expiry will delete it
        kvClient.get(key);

        // Switch to a replicator
        kvClient.put("server1", "b");

        KVMessage response = kvClient.get(key);
        assertEquals(KVMessage.StatusType.GET_ERROR, response.getStatus());
    }

    @Test
    public void testEnableReplicatorsExpireKeys() throws Exception {
        ecs.setReplicatorsExpireKeys(true);

        String key = "server0";
        String value = "a";

        kvClient.putTTL(key, value, 1L);

        // Switch to a replicator
        kvClient.put("server1", "b");

        Thread.sleep(2000);

        KVMessage response = kvClient.get(key);

        assertEquals(KVMessage.StatusType.GET_ERROR, response.getStatus());

        // Cleanup
        ecs.setReplicatorsExpireKeys(false);
    }

    @Test
    public void testDisableReplicatorsExpireKeys() throws Exception {
        String key = "server0";
        String value = "a";

        ecs.setReplicatorsExpireKeys(true);

        // Set back to false
        ecs.setReplicatorsExpireKeys(false);

        kvClient.putTTL(key, value, 1L);

        // Switch to a replicator
        kvClient.put("server1", "b");

        Thread.sleep(2000);

        KVMessage response = kvClient.get(key);

        assertEquals(KVMessage.StatusType.GET_SUCCESS, response.getStatus());
        assertEquals(value, response.getValue());
    }

    @Test
    public void testKeysExpiredEventually() throws Exception {
        // Yes, we're really going to wait 35 seconds in a test

        String key = "server0";
        String value = "a";

        kvClient.putTTL(key, value, 30L);

        // Wait the 30s expiry period
        Thread.sleep(30000);

        KVMessage response = kvClient.get(key);
        assertEquals(KVMessage.StatusType.GET_ERROR, response.getStatus());
    }

    @Test
    public void testKeysExpiredEventuallyOnReplicators() throws Exception {
        String key = "server0";
        String value = "a";

        kvClient.putTTL(key, value, 30L);

        // Wait more than the 30s expiry period
        Thread.sleep(60000);

        // Switch to a replicator
        kvClient.put("server1", "b");
        KVMessage response = kvClient.get(key);
        assertEquals(KVMessage.StatusType.GET_ERROR, response.getStatus());
    }

    @Test
    public void testKeyReplacedWithNoTTL() throws Exception {
        String key = "server0";

        kvClient.putTTL(key, "a", 5L);

        // Replace with non-ttl
        kvClient.put(key, "b");

        // Wait 30s and make sure the key is still there
        Thread.sleep(30000);

        KVMessage response = kvClient.get(key);
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response.getStatus());
        assertEquals("b", response.getValue());

        // Switch to a replicator and check there too
        kvClient.put("server1", "c");

        response = kvClient.get(key);
        assertEquals(KVMessage.StatusType.GET_SUCCESS, response.getStatus());
        assertEquals("b", response.getValue());
    }

    @Test
    public void testKeyReplacedWithTTL() throws Exception {
        String key = "server0";

        kvClient.put(key, "a");

        kvClient.putTTL(key, "b", 2L);

        KVMessage response = kvClient.get(key);
        assertEquals("b", response.getValue());

        // Make sure the key gets expired
        Thread.sleep(3000);

        response = kvClient.get(key);
        assertEquals(KVMessage.StatusType.GET_ERROR, response.getStatus());
    }

}
