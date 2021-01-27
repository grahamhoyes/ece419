package store;

import com.google.gson.Gson;

public class KeyValue {
    private String key;
    private String value;
    private String jsonKV;

    public KeyValue(String key, String value){
        this.key = key;
        this.value = value;
        this.jsonKV = constructFileString();
    }

    public String getKey() {
        return key;
    }

    public String getValue(){
        return value;
    }

    public String getJsonKV() {return jsonKV;}

    public String constructFileString(){
        Gson gson = new Gson();
        return gson.toJson(this) + System.lineSeparator();
    }
}
