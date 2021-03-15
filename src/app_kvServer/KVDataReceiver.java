package app_kvServer;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class KVDataReceiver implements Runnable {
    public static Logger logger = Logger.getLogger("KVDataReceiver");
    private final Socket client;
    private final KVServer kvServer;
    private final int id;

    public KVDataReceiver(int id, KVServer kvServer, Socket client) {
        this.id = id;
        this.client = client;
        this.kvServer = kvServer;
    }

    @Override
    public void run() {
        try {
            logger.info("Receiving data.");
            byte[] buffer = new byte[KVServer.BUFFER_SIZE];

            String tempFileName = kvServer.getDataDir() + File.separatorChar + "~" + id + kvServer.getStorageFile();
            BufferedInputStream socketInput = new BufferedInputStream(client.getInputStream());
            BufferedOutputStream fileOutput = new BufferedOutputStream(new FileOutputStream(tempFileName));

            int size = 0;
            while ((size = socketInput.read(buffer)) > 0) {
                fileOutput.write(buffer, 0, size);
            }

            fileOutput.flush();

            socketInput.close();
            fileOutput.close();
            client.close();

            kvServer.mergeNewData(tempFileName);
        } catch (IOException e) {
            logger.error("Receiving data failed: unable to connect with sender", e);
        }
    }
}
