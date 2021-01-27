package app_kvServer;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class KVServer implements IKVServer, Runnable {

    private static final Logger logger = Logger.getRootLogger();

    private final int port;
    private final int cacheSize;
    private CacheStrategy cacheStrategy;

    private boolean running;
    private ServerSocket serverSocket;


    /**
     * Start KV Server at given port
     *
     * @param port          given port for storage server to operate
     * @param cacheSize     specifies how many key-value pairs the server is allowed
     *                      to keep in-memory
     * @param cacheStrategy specifies the cache replacement strategy in case the cache
     *                      is full and there is a GET- or PUT-request on a key that is
     *                      currently not contained in the cache. Options are "FIFO", "LRU",
     *                      and "LFU". As of Milestone 1, this is unused.
     */
    public KVServer(int port, int cacheSize, String cacheStrategy) {
        this.port = port;
        this.cacheSize = cacheSize;

        try {
            this.cacheStrategy = CacheStrategy.valueOf(cacheStrategy);
        } catch (IllegalArgumentException e) {
            this.cacheStrategy = CacheStrategy.None;
        }
    }

    public static void main(String[] args) {
        // TODO: Expand arg parsing for cache
        try {
            new LogSetup("logs/server.log", Level.ALL);

            if (args.length != 1) {
                System.err.println("Error! Invalid number of arguments");
                System.err.println("Usage: KVServer <port>");
                System.exit(1);
            }

            int port = Integer.parseInt(args[0]);
            KVServer server = new KVServer(port, 0, "None");
            new Thread(server).start();

        } catch (IOException e) {
            System.err.println("Error! Unable to initialize logger.");
            e.printStackTrace();
            System.exit(1);
        }

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
    public boolean inStorage(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean inCache(String key) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public String getKV(String key) throws Exception {
        // TODO Auto-generated method stub
        return "";
    }

    @Override
    public void putKV(String key, String value) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearCache() {
        // TODO Auto-generated method stub
    }

    @Override
    public void clearStorage() {
        // TODO Auto-generated method stub
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
}
