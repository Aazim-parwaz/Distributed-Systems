package com.aazim.kvstore.replication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.aazim.kvstore.model.Hint;
import com.aazim.kvstore.model.ReplicationRequest;
import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.service.KeyValService;

@Component
public class QuorumReplicationStrategy implements ReplicationStrategy {

    private static final Logger log = LoggerFactory.getLogger(QuorumReplicationStrategy.class);

    private final RestTemplate restTemplate;
    private final WebServerApplicationContext context;
    private final KeyValService keyValService;
    private final HintStore hintStore;
    private final ConsistentHashRing ring;

    @Value("${write.quorum:2}")
    private int writeQuorum;

    @Value("${replication.factor:3}")
    private int replicationFactor;

    @Value("${Nodes}")
    private String nodesConfig;

    private String selfNode;

    public QuorumReplicationStrategy(RestTemplate restTemplate, WebServerApplicationContext context,
                                     @Lazy KeyValService keyValService, HintStore hintStore,
                                     ConsistentHashRing ring) {
        this.restTemplate = restTemplate;
        this.context = context;
        this.keyValService = keyValService;
        this.hintStore = hintStore;
        this.ring = ring;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        selfNode = "localhost:" + context.getWebServer().getPort();
        log.info("QuorumReplicationStrategy ready on {}", selfNode);
    }

    @Override
    public boolean replicate(String key, String value, long timestamp, boolean deleted) {
        List<String> preferenceList = ring.getPreferenceList(key, replicationFactor);
        List<String> peers = preferenceList.stream()
                .filter(n -> !n.equals(selfNode))
                .toList();

        int totalNodes   = preferenceList.size();
        int majority     = (totalNodes / 2) + 1;
        int effectiveQuorum = Math.max(Math.min(writeQuorum, totalNodes), majority);

        // Count self as an ACK only if this node is in the preference list for the key.
        int successCount = preferenceList.contains(selfNode) ? 1 : 0;
        log.debug("replicate key={} deleted={} preferenceList={} quorum={}", key, deleted, preferenceList, effectiveQuorum);

        for (String node : peers) {
            try {
                Boolean response = restTemplate.postForObject(
                        "http://" + node + "/kv/internal/replicate",
                        new ReplicationRequest(key, value, timestamp, deleted),
                        Boolean.class);

                if (Boolean.TRUE.equals(response)) {
                    successCount++;
                    if (successCount >= effectiveQuorum) {
                        log.debug("Write quorum achieved {}/{} for key={}", successCount, effectiveQuorum, key);
                        return true;
                    }
                }
            } catch (Exception e) {
                log.warn("Failed to replicate to {}: {}", node, e.getMessage());
                hintStore.store(node, new Hint(key, value, timestamp, deleted));
            }
        }

        boolean met = successCount >= effectiveQuorum;
        if (!met) log.warn("Write quorum NOT met {}/{} for key={}", successCount, effectiveQuorum, key);
        return met;
    }

    @Override
    public List<String> getNodes() {
        return Arrays.stream(nodesConfig.split(","))
                     .map(String::trim)
                     .filter(n -> !n.equals(selfNode))
                     .toList();
    }

    @Override
    public ValueEntry read(String key, ValueEntry localValue) {
        List<String> preferenceList = ring.getPreferenceList(key, replicationFactor);
        List<String> peers = preferenceList.stream()
                .filter(n -> !n.equals(selfNode))
                .toList();

        int readQuorum = (preferenceList.size() / 2) + 1;

        List<CompletableFuture<NodeResponse>> futures = new ArrayList<>();

        // Include self only if this node owns the key.
        if (preferenceList.contains(selfNode)) {
            futures.add(CompletableFuture.completedFuture(new NodeResponse(selfNode, localValue)));
        }

        for (String node : peers) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    ValueEntry entry = restTemplate.getForObject(
                            "http://" + node + "/kv/internal/get?key=" + key, ValueEntry.class);
                    return new NodeResponse(node, entry);
                } catch (Exception e) {
                    log.warn("Failed to read from {}: {}", node, e.getMessage());
                    return null;
                }
            }));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<NodeResponse> responses = futures.stream()
                .map(f -> f.getNow(null))
                .filter(r -> r != null)
                .toList();
        
        // Quorum checked on response size not the valueEntry count because some nodes might be down or unresponsive, and we want to ensure we have enough responses to make a decision even if some are null.
        if (responses.size() < readQuorum) {
            throw new RuntimeException("Read quorum not met: " + responses.size() + "/" + readQuorum
                                       + " for key=" + key);
        }

        List<ValueEntry> validEntries = responses.stream()
                .map(NodeResponse::getEntry)
                .filter(e -> e != null)
                .toList();

        if (validEntries.isEmpty()) return null;

        ValueEntry latest = validEntries.stream()
                .max((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()))
                .orElse(null);

        CompletableFuture.runAsync(() -> repairNodes(key, latest, responses));

        return latest;
    }

    private void repairNodes(String key, ValueEntry latest, List<NodeResponse> responses) {
        for (NodeResponse res : responses) {
            ValueEntry entry = res.getEntry();
            if (entry == null || entry.getTimestamp() < latest.getTimestamp()) {
                if (res.getNode().equals(selfNode)) {
                    keyValService.putInternal(key, latest.getValue(), latest.getTimestamp(), latest.isDeleted());
                } else {
                    try {
                        restTemplate.postForObject(
                                "http://" + res.getNode() + "/kv/internal/replicate",
                                new ReplicationRequest(key, latest.getValue(), latest.getTimestamp(), latest.isDeleted()),
                                Boolean.class);
                        log.info("Read repair sent to {} for key={}", res.getNode(), key);
                    } catch (Exception e) {
                        log.warn("Read repair failed for {}: {}", res.getNode(), e.getMessage());
                    }
                }
            }
        }
    }
}
