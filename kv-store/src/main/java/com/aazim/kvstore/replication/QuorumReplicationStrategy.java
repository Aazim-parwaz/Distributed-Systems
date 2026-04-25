package com.aazim.kvstore.replication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

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

    private final RestTemplate restTemplate;
    private final WebServerApplicationContext context;
    private final KeyValService keyValService;
    private final HintStore hintStore;

    @Value("${write.quorum:2}")
    private int writeQuorum;

    @Value("${Nodes}")
    private String nodesConfig;

    private String selfNode;

    public QuorumReplicationStrategy(RestTemplate restTemplate, WebServerApplicationContext context,
                                     @Lazy KeyValService keyValService, HintStore hintStore) {
        this.restTemplate = restTemplate;
        this.context = context;
        this.keyValService = keyValService;
        this.hintStore = hintStore;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        selfNode = "localhost:" + context.getWebServer().getPort();
        System.out.println("Node started on port: " + context.getWebServer().getPort());
    }

    @Override
    public boolean replicate(String key, String value, long timestamp) {
        List<String> nodes = getNodes();

        int totalNodes = nodes.size() + 1; // +1 for coordinator (promised write)
        int majority = (totalNodes / 2) + 1;
        int effectiveQuorum = Math.max(Math.min(writeQuorum, totalNodes), majority);

        // Start at 1: coordinator will write locally if we return true (promised ACK).
        int successCount = 1;

        System.out.println("Quorum required: " + effectiveQuorum);

        for (String node : nodes) {
            try {
                Boolean response = restTemplate.postForObject(
                    "http://" + node + "/kv/internal/replicate",
                    new ReplicationRequest(key, value, timestamp),
                    Boolean.class);

                if (Boolean.TRUE.equals(response)) {
                    successCount++;
                    System.out.println("Replication successful for " + node);
                    if (successCount >= effectiveQuorum) {
                        System.out.println("Write quorum achieved: " + successCount + "/" + effectiveQuorum);
                        return true;
                    }
                }
            } catch (Exception e) {
                System.err.println("Failed to replicate to " + node + ": " + e.getMessage());
                hintStore.store(node, new Hint(key, value, timestamp));
            }
        }

        System.out.println("Write quorum not met: " + successCount + "/" + effectiveQuorum);
        return successCount >= effectiveQuorum;
    }

    @Override
    public List<String> getNodes() {
        return Arrays.stream(nodesConfig.split(","))
                     .map(String::trim)
                     .filter(node -> !node.equals(selfNode))
                     .toList();
    }

    @Override
    public ValueEntry read(String key, ValueEntry localValue) {
        List<String> nodes = getNodes();
        int totalNodes = nodes.size() + 1;
        int readQuorum = (totalNodes / 2) + 1;

        List<CompletableFuture<NodeResponse>> futures = new ArrayList<>();
        futures.add(CompletableFuture.completedFuture(new NodeResponse(selfNode, localValue)));

        for (String node : nodes) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    ValueEntry entry = restTemplate.getForObject(
                        "http://" + node + "/kv/internal/get?key=" + key, ValueEntry.class);
                    return new NodeResponse(node, entry);
                } catch (Exception e) {
                    System.err.println("Failed to read from " + node + ": " + e.getMessage());
                    return null;
                }
            }));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        List<NodeResponse> responses = futures.stream()
                .map(f -> f.getNow(null))
                .filter(r -> r != null)
                .toList();

        // Availability check: did enough nodes respond?
        if (responses.size() < readQuorum) {
            throw new RuntimeException("Read quorum not met. Responded: " + responses.size() + "/" + readQuorum);
        }

        List<ValueEntry> validEntries = responses.stream()
                .map(NodeResponse::getEntry)
                .filter(e -> e != null)
                .toList();

        // Quorum of nodes responded but none has the key — it genuinely doesn't exist.
        if (validEntries.isEmpty()) {
            return null;
        }

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
                    keyValService.putInternal(key, latest.getValue(), latest.getTimestamp());
                } else {
                    try {
                        restTemplate.postForObject(
                            "http://" + res.getNode() + "/kv/internal/replicate",
                            new ReplicationRequest(key, latest.getValue(), latest.getTimestamp()),
                            Boolean.class);
                        System.out.println("Read repair sent to " + res.getNode());
                    } catch (Exception e) {
                        System.err.println("Read repair failed for " + res.getNode() + ": " + e.getMessage());
                    }
                }
            }
        }
    }
}
