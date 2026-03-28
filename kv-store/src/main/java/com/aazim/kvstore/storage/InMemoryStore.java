package main.java.com.aazim.kvstore.storage;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryStore {
    private final ConcurrentHashMap<String,String> store = new ConcurrentHashMap<>();
    public void put(String key, String value){
        store.put(key,value);
    }
    public String get(String key){
        return store.get(key);
    }
}
