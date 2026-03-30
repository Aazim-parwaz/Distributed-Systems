package com.aazim.kvstore.storage;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;
import java.util.HashMap;
import java.util.Map;


@Component
public class InMemoryStore {
    private final ConcurrentHashMap<String,String> store = new ConcurrentHashMap<>();
    public void put(String key, String value){
        store.put(key,value);
    }
    public String get(String key){
        return store.get(key);
    }
    // return the Most general type(interface)
    public Map<String,String> getAll(){
        return new HashMap<>(store); // return a copy for thread safety
    }
}
