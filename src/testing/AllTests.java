package testing;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import logger.LogSetup;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;


@RunWith(Suite.class)
@Suite.SuiteClasses({
		ConnectionTest.class,
		InteractionTest.class,
		JsonMessageTest.class,
		CommunicationProtocolTest.class
})
public class AllTests {

	@BeforeClass
	public static void setUp() {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			KVServer server = new KVServer(50000, 10, "FIFO");
			server.clearStorage();
			new Thread(server).start();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
