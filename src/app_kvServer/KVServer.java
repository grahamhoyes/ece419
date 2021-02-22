package app_kvServer;

import ecs.HashRing;
import ecs.ECSNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.AdminMessage;
import store.KVSimpleStore;
import store.KVStore;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVServer implements IKVServer, Runnable {

    private static final Logger logger = Logger.getRootLogger();
    public static final Integer BUFFER_SIZE = 1024;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final int port;
    private final int cacheSize;
    private final KVStore kvStore;
    private final CacheStrategy cacheStrategy;
    private boolean running;
    private ServerSocket serverSocket;

    private final String serverName;
    private final ECSConnection ecsConnection;

    private ServerStatus status;


    /**
     * Start KV Server at given port
     *
     * @param port    given port for storage server to operate
     * @param zkHost  ZooKeeper host
     * @param serverName Server name
     * @param zkPort  ZooKeeper port
     */
    public KVServer(int port, String serverName, String zkHost, int zkPort) throws IOException {
        this.port = port;
        this.kvStore = new KVSimpleStore(port + "_store.txt");
        this.status = ServerStatus.STOPPED;
        this.serverName = serverName;

        this.cacheSize = 0;
        this.cacheStrategy = CacheStrategy.None;

        this.ecsConnection = new ECSConnection(zkHost, zkPort, serverName, this);
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public String getHostname() {
        return serverSocket.getInetAddress().getHostName();
    }

    @Override
    public CacheStrategy getCacheStrategy() {
        return cacheStrategy;
    }

    @Override
    public int getCacheSize() {
        return cacheSize;
    }

    public void setStatus(ServerStatus status) {
        this.status = status;
    }

    @Override
    public ServerStatus getStatus() {
        return this.status;
    }

    @Override
    public boolean isNodeResponsible(String key) {
        return ecsConnection.isNodeResponsible(key);
    }

    @Override
    public HashRing getMetadata() {
        return ecsConnection.getHashRing();
    }

    @Override
    public void start() {
        this.status = ServerStatus.ACTIVE;
        logger.info("ServerStatus set to: ACTIVE");
    }

    @Override
    public void stop() {
        this.status = ServerStatus.STOPPED;
        logger.info("ServerStatus set to: STOPPED");
    }

    @Override
    public void lockWrite() {
        this.status = ServerStatus.WRITE_LOCKED;
        logger.info("ServerStatus set to: WRITE_LOCKED");
    }

    @Override
    public void unlockWrite() {
        this.status = ServerStatus.ACTIVE;
        logger.info("ServerStatus set to: ACTIVE");
    }

    @Override
    public String getStorageFile(){
        return port + "_store.txt";
    }

    @Override
    public boolean inStorage(String key) throws Exception {
        boolean exists;
        lock.readLock().lock();
        try {
            exists = this.kvStore.exists(key);
        } finally {
            lock.readLock().unlock();
        }
        return exists;
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        String value;
        lock.readLock().lock();
        try {
            value = this.kvStore.get(key);
        } finally {
            lock.readLock().unlock();
        }
        return value;
    }

    @Override
    public boolean putKV(String key, String value) throws Exception {
        boolean exists;
        lock.writeLock().lock();
        try {
            exists = this.kvStore.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
        return exists;
    }

    @Override
    public void deleteKV(String key) throws Exception {
        lock.writeLock().lock();
        try{
            this.kvStore.delete(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() throws Exception {
        lock.writeLock().lock();
        try {
            this.kvStore.clear();
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void sendData(AdminMessage message){
        lockWrite();
        ECSNode receiveNode = message.getReceiver();
        String host = receiveNode.getNodeHost();
        int port = Integer.parseInt(message.getMessage());
        String[] hashRange = message.getSender().getNodeHashRange();

        try {
            String sendPath = kvStore.splitData(hashRange);
            File sendFile = new File(sendPath);

            byte[] buffer = new byte[BUFFER_SIZE];

            Socket receiveSocket = new Socket(host, port);
            BufferedOutputStream socketOutput = new BufferedOutputStream(receiveSocket.getOutputStream());
            BufferedInputStream fileInput = new BufferedInputStream(new FileInputStream(sendFile));

            int size = 0;
            while ((size = fileInput.read(buffer)) > 0){
                socketOutput.write(buffer, 0, size);
            }

            socketOutput.flush();

            socketOutput.close();
            fileInput.close();
            receiveSocket.close();

            kvStore.sendDataCleanup();
            unlockWrite();

        } catch (IOException e) {
            logger.error("Unable to send data to receiving node.");
            e.printStackTrace();
        }
    }

    private void mergeNewData(){
        lock.writeLock().lock();
        try{
            this.kvStore.mergeData("~" + getStorageFile());
        } catch (IOException e){
            logger.error("Failed to merge incoming data.");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int setupDataReceiver() {
        try {
            ServerSocket receiveSocket = new ServerSocket(0);
            int port = receiveSocket.getLocalPort();
            new Thread(new KVDataReceiver(this, receiveSocket)).start();
            return port;
        } catch (IOException e) {
            logger.error("Unable to open socket to receive data.");
            e.printStackTrace();
            return -1;
        }
    }

    private boolean initializeServer() {
        logger.info("Initializing server...");
        try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " + serverSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            String errorMsg = "Error! Cannot open server socket";

            if (e instanceof BindException) {
                errorMsg += ": Port " + port + " is already bound.";
            }

            logger.error(errorMsg);
        }

        return false;
    }

    @Override
    public void run() {
        running = initializeServer();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();

                    ClientConnection connection = new ClientConnection(client, this);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + getHostname()
                            + ":" + getPort());
                    // TODO: Handle client connections
                } catch (IOException e) {
                    logger.error("Error! Unable to establish connection. \n", e);
                }
            }
        }

        logger.info("Server stopped");
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public void kill() {
        // TODO: Kill client connection threads
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        kill();
    }

    public static void main(String[] args) {
        try{
            if (args.length != 4) {
                System.err.println("Error! Invalid number of arguments");
                System.err.println("Usage: KVServer <port> <server name> <zkHost> <zkPort>");
                System.exit(1);
            }

            int port = Integer.parseInt(args[0]);
            String serverName = args[1];
            String zkHost = args[2];
            int zkPort = Integer.parseInt(args[3]);

            try {
                new LogSetup("logs/server_" + serverName + ".log", Level.ALL);
            } catch (IOException e) {
                System.err.println("Error! Unable to initialize logger.");
                e.printStackTrace();
                System.exit(1);
            }

            KVServer server = new KVServer(port, serverName, zkHost, zkPort);
            new Thread(server).start();
        } catch (IOException e){
            System.err.println("Error! Unable to initialize persistent storage file.");
            e.printStackTrace();
            System.exit(1);
        }

    }

    private class KVDataReceiver implements Runnable{
        private ServerSocket receiveSocket;
        private IKVServer ikvServer;

        public KVDataReceiver(IKVServer ikvServer, ServerSocket receiveSocket){
            this.receiveSocket = receiveSocket;
            this.ikvServer = ikvServer;
        }

        @Override
        public void run() {
            try{
                Socket client = receiveSocket.accept();

                byte[] buffer = new byte[KVServer.BUFFER_SIZE];

                String tempFileName = "~" + ikvServer.getStorageFile();
                BufferedInputStream socketInput = new BufferedInputStream(client.getInputStream());
                BufferedOutputStream fileOutput = new BufferedOutputStream(new FileOutputStream(tempFileName));

                int size = 0;
                while ((size = socketInput.read(buffer)) > 0){
                    fileOutput.write(buffer, 0, size);
                }

                fileOutput.flush();

                socketInput.close();
                fileOutput.close();
                client.close();
                receiveSocket.close();

                mergeNewData();
            } catch (IOException e) {
                logger.error("Receiving data failed: unable to connect with sender", e);
            }
        }

    }

}

