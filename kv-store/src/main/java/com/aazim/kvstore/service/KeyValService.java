package com.aazim.kvstore.service;

import com.aazim.kvstore.clock.LamportClock;
import com.aazim.kvstore.storage.InMemoryStore;
import com.aazim.kvstore.storage.LogStore;
import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.model.LogEntry;
import com.aazim.kvstore.replication.ConsistentHashRing;
import com.aazim.kvstore.replication.ReplicationStrategy;
import com.aazim.kvstore.replication.ReplicationStrategyFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.*;

@Service
public class KeyValService {

    private final InMemoryStore store;
    private final LogStore logStore;
    private final ReplicationStrategyFactory strategyFactory;
    private final LamportClock clock;
    private final ConsistentHashRing ring;
    private final WebServerApplicationContext context;

    @Value("${replication.factor:3}")
    private int replicationFactor;

    private ReplicationStrategy replicationStrategy;
    private String selfNode;

    public KeyValService(InMemoryStore store, LogStore logStore,
                         ReplicationStrategyFactory strategyFactory, LamportClock clock,
                         ConsistentHashRing ring, WebServerApplicationContext context) {
        this.store = store;
        this.logStore = logStore;
        this.strategyFactory = strategyFactory;
        this.clock = clock;
        this.ring = ring;
        this.context = context;
    }

    @PostConstruct
    public void init() {
        this.replicationStrategy = strategyFactory.getStrategy();
        recoverFromLogs();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        selfNode = "localhost:" + context.getWebServer().getPort();
    }

    // Follower write/delete path — timestamp already assigned by coordinator.
    public void putInternal(String key, String value, long ts, boolean deleted) {
        clock.update(ts);
        ValueEntry existing = store.get(key);
        if (existing == null || ts > existing.getTimestamp()) {
            store.put(key, new ValueEntry(value, ts, deleted));
            logStore.append(key, value, ts, deleted);
        }
    }

    // Coordinator write path.
    public boolean put(String key, String value) {
        long ts = clock.tick();
        boolean success = replicationStrategy == null || replicationStrategy.replicate(key, value, ts, false);
        if (success && isOwner(key)) {
            store.put(key, new ValueEntry(value, ts));
            logStore.append(key, value, ts, false);
        }
        return success;
    }

    // Coordinator delete path — replicates a tombstone then stores it locally.
    public boolean delete(String key) {
        long ts = clock.tick();
        boolean success = replicationStrategy == null || replicationStrategy.replicate(key, null, ts, true);
        if (success && isOwner(key)) {
            store.put(key, new ValueEntry(null, ts, true));
            logStore.append(key, null, ts, true);
        }
        return success;
    }

    private boolean isOwner(String key) {
        if (selfNode == null) return true; // startup safety: context not ready yet // temporary inconsistency window where we might accept some writes that should be rejected, but better than losing data by rejecting all writes until ready.
        return ring.getPreferenceList(key, replicationFactor).contains(selfNode);
    }

    public ValueEntry read(String key) {
        ValueEntry localValue = store.get(key);
        ValueEntry result = replicationStrategy != null ? replicationStrategy.read(key, localValue) : localValue;
        return (result != null && result.isDeleted()) ? null : result;
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
                store.put(entry.getKey(), new ValueEntry(entry.getValue(), entry.getTimestamp(), entry.isDeleted()));
            }
        }
        System.out.println("Recovery completed. Loaded keys: " + store.size());
    }
}
