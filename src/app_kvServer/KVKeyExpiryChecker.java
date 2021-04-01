
package app_kvServer;

import org.apache.log4j.Logger;


public class KVKeyExpiryChecker implements Runnable {
    public static Logger logger = Logger.getLogger("KVKeyExpiryChecker");
    private final KVServer kvServer;

    public KVKeyExpiryChecker(KVServer kvServer) {
        this.kvServer = kvServer;
    }

    @Override
    public void run() {
//        logger.info("Key Expiry checker started.");
        if (kvServer.isRunning()) {
            try
            {
                this.kvServer.checkKeyExpiry();
            } catch (Exception e) {
                logger.error("Key expiry checker failed", e);
            }
        }
//        logger.info("Key Expiry checker stopped.");
    }

}
