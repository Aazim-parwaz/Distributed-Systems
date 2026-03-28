package com.aazim.kvstore.storage;

import java.util.HashMap;
import java.util.Map;  // <-- THIS WAS MISSING

public class InMemoryStore {
    private final Map<String, String> store = new HashMap<>();

    public void put(String key, String value) {
        store.put(key, value);
    }

    public String get(String key) {
        return store.get(key);
    }
}