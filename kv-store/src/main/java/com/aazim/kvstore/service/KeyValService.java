package com.aazim.kvstore.service;

import com.aazim.kvstore.clock.LamportClock;
import com.aazim.kvstore.storage.InMemoryStore;
import com.aazim.kvstore.storage.LogStore;
import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.model.LogEntry;
import com.aazim.kvstore.replication.ReplicationStrategy;
import com.aazim.kvstore.replication.ReplicationStrategyFactory;

import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.*;

@Service
public class KeyValService {

    private final InMemoryStore store;
    private final LogStore logStore;
    private final ReplicationStrategyFactory strategyFactory;
    private final LamportClock clock;

    private ReplicationStrategy replicationStrategy;

    public KeyValService(InMemoryStore store, LogStore logStore,
                         ReplicationStrategyFactory strategyFactory, LamportClock clock) {
        this.store = store;
        this.logStore = logStore;
        this.strategyFactory = strategyFactory;
        this.clock = clock;
    }

    @PostConstruct
    public void init() {
        this.replicationStrategy = strategyFactory.getStrategy();
        recoverFromLogs();
        System.out.println("Recovered " + store.size() + " records from log");
    }

    // Follower write path — timestamp already assigned by coordinator.
    public void putInternal(String key, String value, long ts) {
        clock.update(ts);  // advance our clock past what we've seen
        ValueEntry existing = store.get(key);
        if (existing == null || ts > existing.getTimestamp()) {
            store.put(key, new ValueEntry(value, ts));
            logStore.append(key, value, ts);
        }
    }

    // Coordinator write path. Replicates to peers FIRST; only writes locally if
    // quorum is confirmed. Without this ordering, a failed quorum leaves the
    // coordinator with a committed write no client knows about (split-brain).
    public boolean put(String key, String value) {
        long ts = clock.tick();
        boolean success = replicationStrategy == null || replicationStrategy.replicate(key, value, ts);
        if (success) {
            store.put(key, new ValueEntry(value, ts));
            logStore.append(key, value, ts);
        }
        return success;
    }

    public ValueEntry read(String key) {
        ValueEntry localValue = store.get(key);
        if (replicationStrategy != null) {
            return replicationStrategy.read(key, localValue);
        }
        return localValue;
    }

    public Map<String, ValueEntry> getAll() {
        return new HashMap<>(store.getAll());
    }

    public ValueEntry get(String key) {
        return store.get(key);
    }

    private void recoverFromLogs() {
        List<LogEntry> entries = logStore.readAll();
        for (LogEntry entry : entries) {
            ValueEntry existing = store.get(entry.getKey());
            if (existing == null || entry.getTimestamp() > existing.getTimestamp()) {
                store.put(entry.getKey(), new ValueEntry(entry.getValue(), entry.getTimestamp()));
            }
        }
        System.out.println("Recovery completed. Loaded keys: " + store.size());
    }
}
