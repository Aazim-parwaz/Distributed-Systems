package com.aazim.kvstore.service;

import com.aazim.kvstore.storage.InMemoryStore;
import com.aazim.kvstore.storage.LogStore;
import com.aazim.kvstore.replication.ReplicationManager;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.Map;

@Service
public class KeyValService {
    

    private final InMemoryStore store;
    private final LogStore logStore;
    private int writeCount = 0;
    private static final int COMPACTION_THRESHOLD = 5; // compact after every 5 writes

    @Value("${node.role}")
    private String nodeRole; // "leader" or "follower"

    private final ReplicationManager replicationManager;

    public KeyValService(InMemoryStore store, LogStore logStore,ReplicationManager replicationManager) {
        this.store = store;
        this.logStore = logStore;
        this.replicationManager = replicationManager;
    }
    // This runs when app starts
    @PostConstruct
    public void init(){
        Map<String,String> data = logStore.load(); //replay log
        data.forEach(store::put); //load into memory
        System.out.println("Loaded " + data.size() + " records from log");
    }



    public void put(String key, String value){
        store.put(key,value); // fast in-memory write
        logStore.append(key, value); //durability

        //only leader replicates to followers
        if ("leader".equals(nodeRole)){
            replicationManager.replicate(key, value); //replicate to followers
        }
        

        writeCount++;
        if(writeCount >= COMPACTION_THRESHOLD){
            logStore.compact(store.getAll()); // compact log
            writeCount = 0;
            System.out.println("Log compacted");
        }
    }

    public String get(String key){
        return store.get(key);
    }
}
