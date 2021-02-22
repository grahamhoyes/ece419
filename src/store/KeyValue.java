package store;

import com.google.gson.Gson;

import static ecs.ECSNode.md5Hash;

public class KeyValue {
    private String key;
    private String value;
    private String jsonKV;
    private String keyHash;

    public KeyValue(String key, String value){
        this.key = key;
        this.value = value;
        this.keyHash = md5Hash(key);
        this.jsonKV = constructFileString();
    }

    public KeyValue(String key, String value, String keyHash){
        this.key = key;
        this.value = value;
        this.keyHash = keyHash;
        this.jsonKV = constructFileString();
    }

    public String getKey() {
        return key;
    }

    public String getValue(){
        return value;
    }

    public String getKeyHash() {
        if (keyHash == null) {
            keyHash = md5Hash(key);
        }
        return keyHash;
    }

    public String getJsonKV() {
        if (jsonKV == null){
            this.jsonKV = constructFileString();
        }
        return jsonKV;
    }

    public String constructFileString(){
        Gson gson = new Gson();
        return gson.toJson(this) + System.lineSeparator();
    }
}
