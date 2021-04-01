package app_kvServer;

import ecs.HashRing;
import ecs.ServerNode;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.AdminMessage;
import store.KVSimpleStore;
import store.KVStore;
import store.KeyInvalidException;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVServer implements IKVServer, Runnable {

    public static final Integer BUFFER_SIZE = 1024;
    private static final Logger logger = Logger.getLogger("KVServer");
    private static final int NUM_REPLICATORS = 2;

//    private final ReentrantReadWriteLock storageLock = new ReentrantReadWriteLock(true);

    private boolean hasBeenInitialized = false;

    private boolean readyToReplicate = true;
    private final Object replicateSync = new Object();

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
        this.clearReplicatedData();

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

    public Object getReplicateSync() {
        return replicateSync;
    }

    public boolean getReadyToReplicate() {
        return readyToReplicate;
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

    private static int getNonNullLength(ServerNode[] arr) {
        int length = 0;
        for (ServerNode node : arr) {
            if (node != null) {
                length++;
            }
        }
        return length;
    }

    @Override
    public boolean isNodeResponsible(String key) {
        return ecsConnection.isNodeResponsible(key);
    }

    public boolean doesNodeReplicateKey(String key) {
        return ecsConnection.getCurrentNode().doesNodeReplicateKey(key);
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

    @Override
    public String getDataDir() {
        return kvStore.getDataDir();
    }

    public String getWriteLogPath(){
        return kvStore.getWriteLogPath();
    }

    private ReentrantReadWriteLock getWriteLogLock() {
        return kvStore.getWriteLogLock();
    }

    @Override
    public boolean inStorage(String key) throws Exception {
        return this.kvStore.exists(key);
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {

        // If ClientConnection did its job properly, key should be
        // on either this node, or one of the nodes it replicates.
        if (isNodeResponsible(key)) {
            return this.kvStore.get(key);
        } else {
            for (ServerNode node : controllers) {
                if (node.isNodeResponsible(key)) {
                    return this.kvStore.get(key, node);
                }
            }
        }

        throw new KeyInvalidException(key);
    }

    @Override
    public boolean putKV(String key, String value, Long expiryTime) throws Exception {
        boolean exists = this.kvStore.put(key, value, expiryTime);
        updateReplicators();
        return exists;
    }

    public void checkKeyExpiry() throws Exception {
        this.kvStore.checkKeyExpiry();
        updateReplicators();
    }

    @Override
    public void deleteKV(String key) throws Exception {
        this.kvStore.delete(key);
        updateReplicators();
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() throws IOException {
        this.kvStore.clear();
    }

    private void clearReplicatedData() {
        try {
            this.kvStore.initClearReplicatedData();
        } catch (IOException e) {
            logger.error("Failed to clear replicated data", e);
        }
    }

    public void sendData(AdminMessage message) {
        ServerNode receiveNode = message.getReceiver();
        String host = receiveNode.getNodeHost();
        int port = receiveNode.getDataReceivePort();

        HashRing updatedHashRing = message.getMetadata();
        receiveNode = updatedHashRing.getNode(receiveNode.getNodeName());
        String[] sendHashRange = receiveNode.getNodeHashRange();

        try {

            synchronized (replicateSync) {
                // This enforces synchronizing a node sending data to a new node
                // and cleaning up after before sending replicated data to a new node.
                // For cases where there is 1 node, and a new node needs to be sent data
                // but also becomes a replicator.
                readyToReplicate = false;
            }
            logger.info("Starting data transfer to node "
                    + receiveNode.getNodeName()
                    + " at "
                    + host
                    + ": "
                    + port
                    + System.lineSeparator()
                    + "Send Hash Range: "
                    + Arrays.toString(sendHashRange)
            );
            String sendPath = kvStore.splitData(sendHashRange);
            File sendFile = new File(sendPath);

            byte[] buffer = new byte[BUFFER_SIZE];

            Socket receiveSocket = new Socket(host, port);

            BufferedOutputStream socketOutput = new BufferedOutputStream(receiveSocket.getOutputStream());
            BufferedWriter bufferedWriter = new BufferedWriter(
                    new OutputStreamWriter(socketOutput, StandardCharsets.UTF_8));

            BufferedInputStream fileInput = new BufferedInputStream(new FileInputStream(sendFile));
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(fileInput, StandardCharsets.UTF_8));

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
        synchronized (replicateSync){
            readyToReplicate = true;
            replicateSync.notifyAll();
        }
        updateReplicators();
    }

    public void mergeNewData(String tempFilePath) {
        try {
            // This will delete the tempFile if the node is not initialized
            // will not delete if node is initialized, so the tempFile can be sent to the replicator
            this.kvStore.mergeData(tempFilePath, !this.hasBeenInitialized);
            if (!this.hasBeenInitialized) {
                initializeReplicators();
            } else {
                updateReplicators(tempFilePath, new ReentrantReadWriteLock());
            }
            this.hasBeenInitialized = true;
        } catch (IOException e) {
            logger.error("Failed to merge incoming data.");
        }
    }

    public void replicateData(String tempFilePath, String controlServer) {
        kvStore.replicateData(tempFilePath, controlServer);
    }

    private void initializeReplicator(ServerNode replicator) {
        CyclicBarrier deleteBarrier = new CyclicBarrier(1);
        KVDataSender kvDataSender = new KVDataSender(
                replicator,
                this,
                kvStore.getStoragePath(),
                deleteBarrier,
                kvStore.getStorageLock(),
                true);
        new Thread(kvDataSender).start();
    }

    private void initializeReplicators() {
        for (int i = 0; i < NUM_REPLICATORS; i++){
            ServerNode replicator = replicators[i];
            if (replicator != null) {
                initializeReplicator(replicator);
            }
        }
    }

    private void updateReplicators(String filePath, ReentrantReadWriteLock lock) {
        ServerNode serverNode = ecsConnection.getHashRing().getNode(serverName);
        int num_replicators = Math.max(getNonNullLength(replicators), 1);
        CyclicBarrier deleteBarrier = new CyclicBarrier(num_replicators);
        for (int i = 0; i < num_replicators; i++) {
            ServerNode replicator = replicators[i];
            if (replicator != null && replicator.compareTo(serverNode)!=0) {
                logger.info(i + " replicating to " + replicator.getNodeName());
                KVDataSender kvDataSender = new KVDataSender(
                        replicator,
                        this,
                        filePath,
                        deleteBarrier,
                        lock
                );
                new Thread(kvDataSender).start();
            }
        }

    }

    private void updateReplicators() {
        updateReplicators(getWriteLogPath(), getWriteLogLock());
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

        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new KVKeyExpiryChecker(this), 0, 30, TimeUnit.SECONDS);


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
        System.exit(0);
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

        System.exit(0);
    }

    @Override
    public void close() {
        running = false;

        for (ClientConnection connection : connections) {
            connection.close();
        }

        kill();
    }

    public void processServerChange(AdminMessage.ServerChange change, HashRing newHashRing) {
        logger.info("Metadata update: " + change.toString());
        ServerNode[] newReplicators = newHashRing.getReplicators(serverName, NUM_REPLICATORS);
        ServerNode[] newControllers  = newHashRing.getControllers(serverName, NUM_REPLICATORS);

        logger.info("New replicators: " + Arrays.toString(newReplicators));
        logger.info("New controllers: " + Arrays.toString(newControllers));

        switch (change) {
            case ADDED:
                if (!Arrays.equals(newReplicators, replicators)) {
                    for (int i = 0; i < NUM_REPLICATORS; i++) {
                        if (replicators[i] == null || !replicators[i].equals(newReplicators[i])){
                            logger.info("Replicator added: " + newReplicators[i].getNodeName());
                            processNewReplicator(newReplicators[i]);
                            break;
                        }
                    }
                }

                if (!Arrays.equals(newControllers, controllers)) {
                    for (int i = NUM_REPLICATORS-1; i >= 0; i--) {
                        if (controllers [i] == null) {
                            break;
                        } else if (!controllers[i].equals(newControllers[i])) {
                            logger.info("Controller removed: " + controllers[i].getNodeName());
                            processOldController(controllers[i], i, change);
                            break;
                        }
                    }
                }

                break;
            case DELETED:
            case DIED:
                if (!Arrays.equals(newReplicators, replicators)) {
                    for (int i = NUM_REPLICATORS-1; i >= 0; i--) {
                        if (newReplicators[i] == null){
                            break;
                        } else if (replicators[i] == null || !replicators[i].equals(newReplicators[i])){
                            logger.info("Replicator added: " + newReplicators[i].getNodeName());
                            processNewReplicator(newReplicators[i]);
                            break;
                        }
                    }
                }

                if (!Arrays.equals(newControllers, controllers)) {
                    for (int i = 0; i < NUM_REPLICATORS; i++) {
                        if (controllers[i] != null && !controllers[i].equals(newControllers[i])) {
                            logger.info("Controller removed: " + controllers[i].getNodeName());
                            processOldController(controllers[i], i, change);
                            break;
                        }
                    }
                }

                break;
            case STARTED:
            case STOPPED:
            default:
                break;
        }
        replicators = newReplicators;
        controllers = newControllers;
    }

    public void processNewReplicator(ServerNode newReplicator) {
        // newReplicatorIndex is either 0 or 1 (given that NUM_REPLICATORS is 2).
        // 0 means it's the immediate successor, 1 means there's one other replicator between
//        if (newReplicator.compareTo())
        initializeReplicator(newReplicator);
    }

    public void processOldController(ServerNode oldController, int oldControllerIndex, AdminMessage.ServerChange change) {
        // newControllerIndex is either 0 or 1 (given that NUM_REPLICATORS is 2).
        // 0 means it's the immediate predecessor, 1 means there's one other controller between
        if (oldControllerIndex == 0 && change == AdminMessage.ServerChange.DIED) {
            logger.info("Merging replicated data into server data");
            try {
                String replicateFilePath = kvStore.mergeReplicatedData(oldController);
                //TODO: this lock should be specific to this file
                updateReplicators(replicateFilePath, new ReentrantReadWriteLock());
            } catch (Exception e) {
                logger.error("Failed to merge data after node death", e);
            }
        } else {
            logger.info("Deleting replicated data for node: " + oldController.getNodeName());
            kvStore.deleteReplicatedData(oldController);
        }
    }

    public static void main(String[] args) {
        try {
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

