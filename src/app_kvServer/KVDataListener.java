package app_kvServer;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

public class KVDataListener implements Runnable {
    public static Logger logger = Logger.getLogger("KVDataListener");
    private final ServerSocket receiveSocket;
    private final KVServer kvServer;
    private int receiverID = 0;

    public KVDataListener(KVServer kvServer, ServerSocket receiveSocket) {
        this.receiveSocket = receiveSocket;
        this.kvServer = kvServer;
    }

    @Override
    public void run() {
        logger.info("Listener started.");
        while(kvServer.isRunning()) {
            try
            {
                Socket client = receiveSocket.accept();
                KVDataReceiver dataReceiver = new KVDataReceiver(receiverID, kvServer, client);
                new Thread(dataReceiver).start();
                logger.info("Receiving data at "
                        + receiveSocket.getLocalPort()
                        );
                receiverID++;
            } catch (IOException e) {
                logger.error("Receiving data failed: unable to connect with sender", e);
            }
        }
        logger.info("Listener shutdown.");
    }

}
