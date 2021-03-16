package app_kvServer;

import ecs.ServerNode;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVDataSender implements Runnable{
    public static Logger logger = Logger.getLogger("KVDataSender");

    private final ServerNode replicator;
    private final KVServer kvServer;
    public static final Integer BUFFER_SIZE = 1024;


    public KVDataSender(ServerNode replicator, KVServer kvServer){
            this.replicator = replicator;
            this.kvServer = kvServer;
    }

    @Override
    public void run(){
        String host = replicator.getNodeHost();
        int port = replicator.getReplicationReceivePort();

        ReentrantReadWriteLock lock = kvServer.getLock();
        lock.readLock().lock();
        try {
            logger.info("Starting data transfer for replication at node "
                    + replicator.getNodeName()
                    + " at "
                    + host
                    + ": "
                    + port
            );

            File sendFile = new File(kvServer.getStoragePath());

            Socket replicatorSocket = new Socket(host, port);

            BufferedOutputStream socketOutput = new BufferedOutputStream(replicatorSocket.getOutputStream());
            BufferedWriter bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(socketOutput, StandardCharsets.UTF_8));

            BufferedInputStream fileInput = new BufferedInputStream(new FileInputStream(sendFile));
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(fileInput, StandardCharsets.UTF_8));

            String controlServer = kvServer.getServerName() + System.lineSeparator();
            socketOutput.write(controlServer.getBytes(StandardCharsets.UTF_8));

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line + System.lineSeparator());
            }

            bufferedWriter.flush();
            bufferedWriter.close();
            bufferedReader.close();
            socketOutput.close();
            fileInput.close();
            replicatorSocket.close();

            logger.info("Finished data transfer for replication at node " + replicator.getNodeName());

        } catch (IOException e) {
            logger.error("Unable to send data to receiving node", e);
        } finally {
            lock.readLock().unlock();
        }
    }
}
