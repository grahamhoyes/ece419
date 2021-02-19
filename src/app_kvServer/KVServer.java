package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import store.KVSimpleStore;
import store.KVStore;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class KVServer implements IKVServer, Runnable {

    private static final Logger logger = Logger.getRootLogger();

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
    public void deleteKV(String key) throws Exception{
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
        try {
            new LogSetup("logs/server.log", Level.ALL);
        } catch (IOException e) {
            System.err.println("Error! Unable to initialize logger.");
            e.printStackTrace();
            System.exit(1);
        }

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

            KVServer server = new KVServer(port, serverName, zkHost, zkPort);
            new Thread(server).start();
        } catch (IOException e){
            System.err.println("Error! Unable to initialize persistent storage file.");
            e.printStackTrace();
            System.exit(1);
        }

    }
}
