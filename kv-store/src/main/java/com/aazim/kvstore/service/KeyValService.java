package com.aazim.kvstore.service;

import com.aazim.kvstore.storage.InMemoryStore;
import com.aazim.kvstore.storage.LogStore;
import com.aazim.kvstore.storage.FileLogStore;
import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.replication.ReplicationStrategy;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

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

    private final RestTemplate restTemplate = new RestTemplate();
    
    public KeyValService(LogStore logStore, ReplicationStrategyFactory strategyFactory){
        this.logStore = logStore;
        this.strategyFactory = strategyFactory;
    }

    @PostConstruct
    public void init(){
        
        //init replication strategy based on config
        this.replicationStrategy = strategyFactory.getStrategy();
        recoverFromLogs();
        System.out.println("Recovered " + store.size() + " records from log");

    }
    public boolean handleWrite(String key, String value){
        long ts = System.currentTimeMillis();

        putInternal(key, value, ts);
        return replicationStrategy != null && replicationStrategy.handleWrite(key, value, ts); //Deligate write handling to strategy
        
    }

    public boolean put(String key, String value){
        long ts = System.currentTimeMillis();

        store.put(key, new ValueEntry(value, ts)); // fast in-memory write
        logStore.append(key, value,ts); //durability
        boolean success = replicationStrategy != null && replicationStrategy.handleWrite(key, value, ts); //Deligate write handling to strategy

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
    public void putInternal(String key, String value, long ts){
        ValueEntry existing = store.get(key);

        if (existing == null || ts > existing.getTimestamp()){
            store.put(key, new ValueEntry(value, ts)); // fast in-memory write
            logStore.append(key, value,ts); //durability
        }
    }
    // internal read
    public ValueEntry getValueEntry(String key){
        return store.get(key);
    }
    // read quorum + read repair
    public String handleRead(String key){
        
        List<String> nodes = replicationStrategy.getNodes();

        int totalNodes = nodes.size() + 1; // including self
        int readQuorum = (totalNodes / 2) + 1; // majority

        List<ValueEntry> response = new ArrayList<>();

        ValueEntry localEntry = store.get(key);
        if(localEntry != null){
            response.add(localEntry);
        }

        for(String node:nodes){
            try {
                ValueEntry entry = restTemplate.getForObject("http://"+node+"/kv/internal/get?key={k}", ValueEntry.class, key);
                if (entry != null){
                    response.add(entry);
                }
            } catch (Exception e) {
                System.err.println("Failed to fetch from " + node + ": " + e.getMessage());
            }
        }

        // quorum check
        if (response.size() < readQuorum){
            throw new RuntimeException("Read quorum not met. Available replicas: " + response.size() + "/" + readQuorum);
        }


        //Last write wins
        ValueEntry latest = response.stream().max(Comparator.comparingLong(ValueEntry::getTimestamp)).orElse(null);
        if (latest ==null) return null;

        //Read repair
        for (String node:nodes){
            try {
                ValueEntry entry = restTemplate.getForObject("http://"+node+"/kv/internal/get?key={k}", ValueEntry.class, key);
                if (entry == null || entry.getTimestamp() < latest.getTimestamp()){
                    // send repair
                    restTemplate.postForObject("http://"+node+"/kv/internal/replicate?key={k}&value={v}&ts={t}", null, String.class, key, latest.getValue(), latest.getTimestamp());
                    System.out.println("Sent read repair to " + node + ": " + key + "=" + latest.getValue());
                }
            } catch (Exception e) {
                System.err.println("Failed to send read repair to " + node + ": " + e.getMessage());
            }
        }
        return latest.getValue();

        

        }
    }

}
