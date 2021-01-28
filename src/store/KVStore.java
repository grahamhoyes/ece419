package store;

public interface KVStore {
    public boolean put(String key, String value) throws Exception;
    public String get(String key) throws Exception;
    public void clear() throws Exception;
    public boolean exists(String key) throws Exception;
}