package testing;

import java.net.UnknownHostException;

import client.KVStoreConnection;

import ecs.ECS;
import junit.framework.TestCase;
import org.junit.*;


/**
 * Tests for the connection to the KVStone
 *
 * Written by the course instructors, and reformatted to conform
 * to Junit 4 standards. The poor conventions around exception
 * handling are left as-is.
 */
public class ConnectionTest extends Assert {

	private static ECS ecs;
	private static final String zkHost = "127.0.0.1";
	private static final int zkPort = 2181;

	@BeforeClass
	public static void setUp() {
		ecs = new ECS("ecs_test.config", zkHost, zkPort, System.getenv("KV_SERVER_JAR"));
		ecs.addNodes(1);
	}

	@AfterClass
	public static void shutDownNodes() {
		ecs.shutdown();
	}

	@Test()
	public void testConnectionSuccess() throws Exception{
		KVStoreConnection kvClient = new KVStoreConnection("localhost", 9000);
		kvClient.connect();
	}

	@Test()
	public void testUnknownHost() {
		Exception ex = null;
		KVStoreConnection kvClient = new KVStoreConnection("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}

	@Test()
	public void testIllegalPort() {
		Exception ex = null;
		KVStoreConnection kvClient = new KVStoreConnection("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

