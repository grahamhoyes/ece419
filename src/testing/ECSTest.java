package testing;

import client.KVStoreConnection;
import ecs.ECS;
import ecs.ZooKeeperConnection;
import junit.framework.TestCase;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.zookeeper.ZooKeeper;
import org.junit.*;

import java.io.IOException;

public class ECSTest extends Assert {
    private static ECS ecs;
    private static String zkHost = "127.0.0.1";
    private static int zkPort = 2181;

    @BeforeClass
    public static void setUp() {
        ecs = new ECS("ecs_test.config", zkHost, zkPort, "localhost");
    }

    @AfterClass
    public static void tearDown() {
        ecs.shutdown();
    }
    @Test()
    public void testSingleServerStartup() throws Exception{
        ecs.addNodes(1);

        Exception ex = null;
        KVStoreConnection kvClient = new KVStoreConnection("localhost", 9000);

        try {
            kvClient.connect();
        } catch (Exception e) {
            ex = e;
        }
        assertNull(ex);

    }
}
