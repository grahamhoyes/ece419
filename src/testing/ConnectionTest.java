package testing;

import java.net.UnknownHostException;

import client.KVStoreConnection;

import junit.framework.TestCase;


/**
 * Tests for the connection to the KVStone
 *
 * Written by the course instructors, and reformatted to conform
 * to Junit 4 standards. The poor conventions around exception
 * handling are left as-is.
 */
public class ConnectionTest extends TestCase {

	
	public void testConnectionSuccess() {
		
		Exception ex = null;
		
		KVStoreConnection kvClient = new KVStoreConnection("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		
		assertNull(ex);
	}
	
	
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

