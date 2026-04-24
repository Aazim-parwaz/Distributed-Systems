package com.aazim.kvstore.storage;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

import com.aazim.kvstore.model.ValueEntry;

import java.util.HashMap;
import java.util.Map;

@Component
public class InMemoryStore {
    private final ConcurrentHashMap<String,ValueEntry> store = new ConcurrentHashMap<>();
    public void put(String key, ValueEntry value){
        store.put(key,value);
    }

    public ValueEntry get(String key){
        return store.get(key);
    }
    // return the Most general type(interface)
    public Map<String,ValueEntry> getAll(){
        return new HashMap<>(store); // return a copy for thread safety
    }
    public int size(){
        return store.size();
    }
}
