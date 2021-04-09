package testing;

import org.apache.log4j.Level;

import logger.LogSetup;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
		ConnectionTest.class,
		InteractionTest.class,
		JsonMessageTest.class,
		KVStoreTest.class,
		CommunicationProtocolTest.class,
		HashRingTest.class,
		ECSTest.class,
		KeyExpiryTest.class
})
public class AllTests {

	@BeforeClass
	public static void setUp() {
		try {
			new LogSetup("logs/testing/test.log", Level.ALL);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
