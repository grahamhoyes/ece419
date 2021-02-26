package testing;

import ecs.ECS;
import org.junit.*;

import client.KVStoreConnection;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;


/**
 * Tests for interaction with the KVStore
 *
 * Written by the course instructors, and reformatted to conform
 * to Junit 4 standards. The poor conventions around exception
 * handling are left as-is.
 */
public class InteractionTest extends Assert {

	private KVStoreConnection kvClient;
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
	
	@Before
	public void testSetUp() throws Exception{
		kvClient = new KVStoreConnection("localhost", 9000);
		kvClient.connect();
	}

	@After
	public void tearDown() {
		kvClient.disconnect();
	}
	
	
	@Test
	public void testPut() {
		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
	}
	
	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNotNull(ex);
	}

	@Test
	public void testUpdate() throws Exception {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;

		kvClient.put(key, initialValue);
		response = kvClient.put(key, updatedValue);


		Assert.assertTrue(response.getStatus() == StatusType.PUT_UPDATE
				&& response.getValue().equals(updatedValue));
	}
	
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_SUCCESS);
	}
	
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}
	


}
