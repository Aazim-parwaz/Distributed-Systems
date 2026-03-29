package com.aazim.kvstore.service;

import com.aazim.kvstore.storage.InMemoryStore;
import com.aazim.kvstore.storage.LogStore;

import org.springframework.stereotype.Service;

@Service
public class KeyValService {
    private final InMemoryStore store;
    private final LogStore logStore;

    public KeyValService(InMemoryStore store, LogStore logStore) {
        this.store = store;
        this.logStore = logStore;
    }
    public void put(String key, String value){
        store.put(key,value); // fast path
        logStore.append(key, value); //durability
    }

    public String get(String key){
        return store.get(key);
    }
}
