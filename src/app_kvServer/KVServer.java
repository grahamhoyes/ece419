package app_kvServer;

import ecs.HashRing;
import ecs.ServerNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.AdminMessage;
import store.KVSimpleStore;
import store.KVStore;

import java.io.*;
import java.math.BigInteger;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVServer implements IKVServer, Runnable {

    public static final Integer BUFFER_SIZE = 1024;
    private static final Logger logger = Logger.getLogger("KVServer");
    private static final int NUM_REPLICATORS = 2;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final int port;
    private final int cacheSize;

    private final KVStore kvStore;
    private final CacheStrategy cacheStrategy;

    private final String serverName;
    private final ECSConnection ecsConnection;

    private int dataReceivePort;
    private int replicationReceivePort;

    private boolean running;

    private ServerSocket serverSocket;
    private ServerSocket dataReceiveSocket;
    private ServerSocket replicationReceiveSocket;

    private final ArrayList<ClientConnection> connections = new ArrayList<>();

    private ServerStatus status;
    private ServerNode[] replicators = new ServerNode[NUM_REPLICATORS];
    private ServerNode[] controllers = new ServerNode[NUM_REPLICATORS];


    /**
     * Start KV Server at given port
     *
     * @param port       given port for storage server to operate
     * @param zkHost     ZooKeeper host
     * @param serverName Server name
     * @param zkPort     ZooKeeper port
     */
    public KVServer(int port, String serverName, String zkHost, int zkPort) throws IOException {
        this.port = port;
        this.kvStore = new KVSimpleStore(serverName);
        this.status = ServerStatus.STOPPED;
        this.serverName = serverName;

        this.cacheSize = 0;
        this.cacheStrategy = CacheStrategy.None;

        this.clearStorage();

        this.acquireReceivingPorts();

        this.ecsConnection = new ECSConnection(zkHost, zkPort, serverName, this);
    }

    @Override
    public int getPort() {
        return port;
    }

    public int getReplicationReceivePort() {
        return replicationReceivePort;
    }

    public int getDataReceivePort() {
        return dataReceivePort;
    }

    public ReentrantReadWriteLock getLock() {
        return lock;
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

    @Override
    public ServerStatus getStatus() {
        return this.status;
    }

    public void setStatus(ServerStatus status) {
        this.status = status;
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

    public String getServerName() {
        return serverName;
    }

    @Override
    public String getStorageFile() {
        return kvStore.getFileName();
    }

    public String getStoragePath() {
        return kvStore.getStoragePath();
    }

    @Override
    public String getDataDir() {
        return kvStore.getDataDir();
    }

    public String getWriteLogPath(){
        return kvStore.getWriteLogPath();
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
        updateReplicators();
        return exists;
    }

    @Override
    public void deleteKV(String key) throws Exception {
        lock.writeLock().lock();
        try {
            this.kvStore.delete(key);
        } finally {
            lock.writeLock().unlock();
        }
        updateReplicators();
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() throws IOException {
        lock.writeLock().lock();
        try {
            this.kvStore.clear();
        } finally {
            lock.writeLock().unlock();
        }

    }

    public void sendData(AdminMessage message) {
        ServerNode receiveNode = message.getReceiver();
        String host = receiveNode.getNodeHost();
        int port = receiveNode.getDataReceivePort();

        BigInteger startingHashValue = new BigInteger(receiveNode.getNodeHash(), 16)
                .add(BigInteger.ONE)
                .mod(ServerNode.HASH_MAX);
        StringBuilder startingHash = new StringBuilder(startingHashValue.toString(16));
        while (startingHash.length() < 32) {
            startingHash.insert(0, "0");
        }

        String[] hashRange = new String[]{startingHash.toString(), message.getSender().getNodeHash()};

        try {
            logger.info("Starting data transfer to node "
                    + receiveNode.getNodeName()
                    + " at "
                    + host
                    + ": "
                    + port
                    + System.lineSeparator()
            );
            String sendPath = kvStore.splitData(hashRange);
            File sendFile = new File(sendPath);

            byte[] buffer = new byte[BUFFER_SIZE];

            Socket receiveSocket = new Socket(host, port);

            BufferedOutputStream socketOutput = new BufferedOutputStream(receiveSocket.getOutputStream());
            BufferedWriter bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(socketOutput, StandardCharsets.UTF_8));

            BufferedInputStream fileInput = new BufferedInputStream(new FileInputStream(sendFile));
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(fileInput, StandardCharsets.UTF_8));

//            int size;
//            while ((size = fileInput.read(buffer)) > 0) {
//                socketOutput.write(buffer, 0, size);
//            }
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                bufferedWriter.write(line + System.lineSeparator());
            }

            bufferedWriter.flush();
            bufferedWriter.close();
            bufferedReader.close();

            socketOutput.close();
            fileInput.close();
            receiveSocket.close();

            logger.info("Finished data transfer to node " + receiveNode.getNodeName());

        } catch (IOException e) {
            logger.error("Unable to send data to receiving node", e);
        }
    }

    public void cleanUpData() {
        kvStore.sendDataCleanup();
    }

    public void mergeNewData(String tempFilePath) {
        lock.writeLock().lock();
        try {
            this.kvStore.mergeData(tempFilePath);
        } catch (IOException e) {
            logger.error("Failed to merge incoming data.");
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void replicateData(String tempFilePath, String controlServer) {
        kvStore.replicateData(tempFilePath, controlServer);
    }

    public void updateReplicators() {
        ServerNode serverNode = ecsConnection.getHashRing().getNode(serverName);

        //TODO: check array of replicators instead of doing it through successors
        ServerNode replicator = serverNode.getSuccessor();
        for (int i = 0; i < NUM_REPLICATORS; i++){
            if (replicator.compareTo(serverNode)!=0){
                KVDataSender kvDataSender = new KVDataSender(replicator, this);
                new Thread(kvDataSender).start();
            }
            replicator = replicator.getSuccessor();
        }
    }

    private void acquireReceivingPorts() {
        try {
            this.replicationReceiveSocket = new ServerSocket(0);
            this.dataReceiveSocket = new ServerSocket(0);

            this.replicationReceivePort = this.replicationReceiveSocket.getLocalPort();
            this.dataReceivePort = this.dataReceiveSocket.getLocalPort();
        } catch (IOException e) {
            logger.error("Unable to open socket to receive data.");
            e.printStackTrace();
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

    private void initializeDataListeners() {
        new Thread(new KVDataListener(this, replicationReceiveSocket, true)).start();
        new Thread(new KVDataListener(this, dataReceiveSocket, false)).start();
    }

    @Override
    public void run() {
        running = initializeServer();

        initializeDataListeners();

        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();

                    ClientConnection connection = new ClientConnection(client, this);
                    connections.add(connection);
                    new Thread(connection).start();

                    logger.info("Connected to "
                            + getHostname()
                            + ":" + getPort());

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
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.error("Error! Unable to close socket on port: " + port, e);
        }
    }

    @Override
    public void close() {
        running = false;

        for (ClientConnection connection : connections) {
            connection.close();
        }

        kill();
    }

    public void processServerChange(AdminMessage.ServerChange change, ServerNode changedNode, HashRing newHashRing) {
        switch (change) {
            case ADDED:
            case DELETED:
            case DIED:

                ServerNode[] newReplicators = newHashRing.getReplicators(serverName, NUM_REPLICATORS);
                ServerNode[] newControllers  = newHashRing.getControllers(serverName, NUM_REPLICATORS);

                if (!Arrays.equals(newReplicators, replicators)) {
                    for (int i=0; i<NUM_REPLICATORS; i++) {
                        ServerNode node = newReplicators[i];
                        if (node.equals(changedNode)) {
                            processNewReplicator(changedNode, i, change);
                            break;
                        }
                    }
                }

                if (!Arrays.equals(newControllers, controllers)) {
                    for (int i=0; i<NUM_REPLICATORS; i++) {
                        ServerNode node = newControllers[i];
                        if (node.equals(changedNode)) {
                            processNewController(changedNode, controllers[NUM_REPLICATORS-1], i, change);
                            break;
                        }
                    }
                }

                replicators = newReplicators;
                controllers = newControllers;

                break;
            case STARTED:
            case STOPPED:
            default:
                break;
        }
    }

    public void processNewReplicator(ServerNode newReplicator, int newReplicatorIndex, AdminMessage.ServerChange change) {
        // TODO
        // newReplicatorIndex is either 0 or 1 (given that NUM_REPLICATORS is 2).
        // 0 means it's the immediate successor, 1 means there's one other replicator between
    }

    public void processNewController(ServerNode newController, ServerNode oldController, int newControllerIndex, AdminMessage.ServerChange change) {
        // TODO
        // newControllerIndex is either 0 or 1 (given that NUM_REPLICATORS is 2).
        // 0 means it's the immediate predecessor, 1 means there's one other controller between
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

}

