package store;

import java.io.IOException;

public interface KVStore {
    public boolean put(String key, String value) throws Exception;
    public String get(String key) throws Exception;
    public void clear() throws IOException;
    public boolean exists(String key) throws Exception;
    public void delete(String key) throws Exception;

    void mergeData(String newFileName) throws IOException;
    String splitData(String[] hashRange) throws IOException;
    void sendDataCleanup();

    public String getFileName();
    public String getDataDir();

    void replicateData(String tempFilePath, String controlServer);

    String getStoragePath();

    String getWriteLogPath();
}