package com.aazim.kvstore.service;

import com.aazim.kvstore.storage.InMemoryStore;
import com.aazim.kvstore.storage.LogStore;
import com.aazim.kvstore.storage.FileLogStore;
import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.replication.ReplicationStrategy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.aazim.kvstore.replication.ReplicationStrategyFactory;
import com.aazim.kvstore.model.LogEntry;

import jakarta.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class KeyValService {

    private final Map<String,ValueEntry> store = new ConcurrentHashMap<>();

    @Autowired
    private LogStore logStore;

    @Autowired
    private ReplicationStrategyFactory strategyFactory;

    @Value("${Nodes}")
    private String nodesConfig;

    private ReplicationStrategy replicationStrategy;

    @PostConstruct
    public void init(){
        
        //init replication strategy based on config
        this.replicationStrategy = strategyFactory.getStrategy();
        recoverFromLogs();
        System.out.println("Recovered " + store.size() + " records from log");

    }
    public boolean replicate(String key, String value){
        long ts = System.currentTimeMillis();

        putInternal(key, value, ts);
        return replicationStrategy != null && replicationStrategy.replicate(key, value, ts); //Deligate write handling to strategy
        
    }
    public void putInternal(String key, String value, long ts){
        ValueEntry existing = store.get(key);

        if (existing == null || ts > existing.getTimestamp()){
            store.put(key, new ValueEntry(value, ts)); // fast in-memory write
            logStore.append(key, value,ts); //durability
        }
    }

    public boolean put(String key, String value){
        long ts = System.currentTimeMillis();

        store.put(key, new ValueEntry(value, ts)); // fast in-memory write
        logStore.append(key, value,ts); //durability
        boolean success = replicationStrategy != null && replicationStrategy.replicate(key, value, ts); //Deligate write handling to strategy

        return success;
        
    }

    public ValueEntry get(String key){
        return store.get(key);
    }

    public Map<String,ValueEntry> getAll(){
        return new HashMap<>(store); // return a copy for thread safety
    }

    private void recoverFromLogs(){
        List<LogEntry> entries = logStore.readAll();

        for (LogEntry entry: entries){
            ValueEntry existing = store.get(entry.getKey());

            if(existing == null || entry.getTimestamp() > existing.getTimestamp()){
                store.put(entry.getKey(), new ValueEntry(entry.getValue(), entry.getTimestamp()));
            }
        }
        System.out.println("Recovery completed. Loaded keys: "+ store.size());
    }
    public ValueEntry read(String key){
        ValueEntry localValue = store.get(key);
        System.out.println("Local read for key: " + key + ", value: " + (localValue != null ? localValue.getValue() : "null"));
        if (replicationStrategy != null){
            System.out.println("Performing strategy-based read for key: " + key);
            return replicationStrategy.read(key, localValue);
        }
        return localValue;
    
    }
    public ValueEntry getValueEntry(String key){
        return store.get(key);
    }
}

