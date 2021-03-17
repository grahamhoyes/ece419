package app_kvServer;

import org.apache.log4j.Logger;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class KVDataReceiver implements Runnable {
    public static Logger logger = Logger.getLogger("KVDataReceiver");
    private final Socket client;
    private final KVServer kvServer;
    private final int id;
    private boolean replicator;

    public KVDataReceiver(int id, KVServer kvServer, Socket client, boolean replicator) {
        this.id = id;
        this.client = client;
        this.kvServer = kvServer;
        this.replicator = replicator;
    }

    @Override
    public void run() {
        try {

            BufferedInputStream socketInput = new BufferedInputStream(client.getInputStream());
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(socketInput, StandardCharsets.UTF_8));
            Path tempPath = Files.createTempFile(String.valueOf(id) + kvServer.getServerName(), ".txt");
            String tempFileName = tempPath.toString();
            String controlServer = "";

            if (replicator) {
                controlServer = bufferedReader.readLine();
                logger.info("Receiving data to replicate from "
                        + controlServer
                        + " at file: "
                        + tempFileName
                );
            } else {
                logger.info("Receiving data");
            }

            BufferedOutputStream fileOutput = new BufferedOutputStream(new FileOutputStream(tempFileName));
            BufferedWriter bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(fileOutput, StandardCharsets.UTF_8));

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line + System.lineSeparator());
            }

            bufferedWriter.close();
            fileOutput.flush();

            socketInput.close();
            fileOutput.close();
            client.close();

            if (replicator) {
                kvServer.replicateData(tempFileName, controlServer);
            } else {
                kvServer.mergeNewData(tempFileName);
            }
        } catch (IOException e) {
            logger.error("Receiving data failed: unable to connect with sender", e);
        }
    }
}
