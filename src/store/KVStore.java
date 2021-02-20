package store;

import java.io.IOException;

public interface KVStore {
    public boolean put(String key, String value) throws Exception;
    public String get(String key) throws Exception;
    public void clear() throws Exception;
    public boolean exists(String key) throws Exception;
    public void delete(String key) throws Exception;

    void mergeData(String newFileName) throws IOException;
}