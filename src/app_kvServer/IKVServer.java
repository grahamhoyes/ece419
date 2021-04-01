package app_kvServer;

import ecs.HashRing;

import java.io.IOException;

public interface IKVServer {
    public enum CacheStrategy {
        None,
        LRU,
        LFU,
        FIFO
    };

    public enum ServerStatus {
        OFFLINE,        // Not started yet
        STOPPED,        // Not accepting connections
        ACTIVE,         // Accepting connections
        WRITE_LOCKED,   // Not accepting write requests
    }

    /**
     * Get the port number of the server
     * @return  port number
     */
    public int getPort();

    /**
     * Get the hostname of the server
     * @return  hostname of server
     */
    public String getHostname();

    /**
     * Get the cache strategy of the server
     * @return  cache strategy
     */
    public CacheStrategy getCacheStrategy();

    /**
     * Get the cache size
     * @return  cache size
     */
    public int getCacheSize();


    /**
     * Return the storage file name.
     * @return storage file
     */
    public String getStorageFile();

    public String getDataDir();

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inStorage(String key) throws  Exception;

    /**
     * Check if key is in storage.
     * NOTE: does not modify any other properties
     * @return  true if key in storage, false otherwise
     */
    public boolean inCache(String key);

    /**
     * Get the value associated with the key
     * @return  value associated with key
     * @throws Exception
     *      when key not in the key range of the server
     */
    public String getKV(String key) throws Exception;

    /**
     * Put the key-value pair into storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public boolean putKV(String key, String value, Long ttl) throws Exception;

    /**
     * Delete the key-value pair in storage
     * @throws Exception
     *      when key not in the key range of the server
     */
    public void deleteKV(String key) throws Exception;

    /**
     * Clear the local cache of the server
     */
    public void clearCache();

    /**
     * Clear the storage of the server
     */
    public void clearStorage() throws IOException;

    /**
     * Starts running the server
     */
    public void run();

    /**
     * Abruptly stop the server without any additional actions
     * NOTE: this includes performing saving to storage
     */
    public void kill();

    /**
     * Gracefully stop the server, can perform any additional actions
     */
    public void close();

    /**
     * Allow the server to start receiving requests
     */
    public void start();

    /**
     * Stop the server from receiving requests
     */
    public void stop();

    /**
     * Lock the server from receiving write requests
     */
    public void lockWrite();

    /**
     * Remove the write lock
     */
    public void unlockWrite();

    /**
     * Determine if the given key falls within this server's range
     */
    public boolean isNodeResponsible(String key);

    /*
     * Get metadata of the cluster
     */
    public HashRing getMetadata();

    public ServerStatus getStatus();
}
