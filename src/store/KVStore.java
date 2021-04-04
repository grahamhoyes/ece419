package store;

import ecs.ServerNode;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public interface KVStore {
    public boolean put(String key, String value, Long expiryTime) throws Exception;
    public String get(String key) throws Exception;
    public String get(String key, ServerNode responsibleNode) throws Exception;
    public void clear() throws IOException;
    public boolean exists(String key) throws Exception;

    void initClearReplicatedData() throws IOException;

    public void delete(String key) throws Exception;

    void mergeData(String newFileName, boolean deleteFile) throws IOException;

    void mergeData(String newFileName) throws IOException;
    String splitData(String[] hashRange) throws IOException;
    void sendDataCleanup();

    public String getFileName();
    public String getDataDir();

    void replicateData(String tempFilePath, String controlServer);

    String getStoragePath();

    String getWriteLogPath();

    String mergeReplicatedData(ServerNode controller) throws Exception;

    void deleteReplicatedData(ServerNode oldController);

    ReentrantReadWriteLock getWriteLogLock();

    ReentrantReadWriteLock getStorageLock();

    boolean checkKeyExpiry() throws Exception;
}