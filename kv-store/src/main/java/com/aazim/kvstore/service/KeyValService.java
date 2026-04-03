package com.aazim.kvstore.service;

import com.aazim.kvstore.storage.InMemoryStore;
import com.aazim.kvstore.storage.LogStore;
import com.aazim.kvstore.replication.ReplicationManager;
import com.aazim.kvstore.replication.ReplicationStrategy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.aazim.kvstore.replication.ReplicationStrategyFactory;

import jakarta.annotation.PostConstruct;
import java.util.Map;

@Service
public class KeyValService {
    

    private final InMemoryStore store;
    private final LogStore logStore;
    

    @Value("${node.role}")
    private String nodeRole; // "leader" or "follower"
    

    @Autowired
    private ReplicationStrategyFactory replicationStrategyFactory;

    private ReplicationStrategy replicationStrategy;

    @PostConstruct
    public void init(){
        // load state from log
        Map<String,String> data = logStore.load(); //replay log
        data.forEach(store::put); //load into memory
        

        //init replication strategy based on config
        this.replicationStrategy = replicationStrategyFactory.getStrategy();

        System.out.println("Recovered " + data.size() + " records from log");

    }


    public KeyValService(InMemoryStore store, LogStore logStore,ReplicationManager replicationManager) {
        this.store = store;
        this.logStore = logStore;
    }
    
    public void put(String key, String value){
        store.put(key,value); // fast in-memory write
        logStore.append(key, value); //durability

        //only leader replicates to followers
        if ("leader".equals(nodeRole)){
            replicationStrategy.replicate(key, value); //replicate to followers
        }
        
    }
    public String get(String key){
        return store.get(key);
    }
}
