package com.aazim.kvstore.replication;

import java.util.ArrayList;
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

import com.aazim.kvstore.model.NodeState;
import com.aazim.kvstore.model.ReplicationRequest;
import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.service.KeyValService;

@Component
public class AsyncReplicationStrategy implements ReplicationStrategy {

    private static final Logger log = LoggerFactory.getLogger(AsyncReplicationStrategy.class);

    private final RestTemplate restTemplate;
    private final WebServerApplicationContext context;
    private final AsyncSender asyncSender;
    private final KeyValService keyValService;
    private final ConsistentHashRing ring;
    private final GossipService gossipService;

    @Value("${replication.factor:3}")
    private int replicationFactor;

    private String selfNode;

    public AsyncReplicationStrategy(RestTemplate restTemplate, WebServerApplicationContext context,
                                    AsyncSender asyncSender, @Lazy KeyValService keyValService,
                                    ConsistentHashRing ring, GossipService gossipService) {
        this.restTemplate = restTemplate;
        this.context = context;
        this.asyncSender = asyncSender;
        this.keyValService = keyValService;
        this.ring = ring;
        this.gossipService = gossipService;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        selfNode = "localhost:" + context.getWebServer().getPort();
    }

    @Override
    public boolean replicate(String key, String value, long timestamp, boolean deleted) {
        ring.getPreferenceList(key, replicationFactor).stream()
            .filter(n -> !n.equals(selfNode))
            .forEach(node -> asyncSender.send(node, new ReplicationRequest(key, value, timestamp, deleted)));
        return true;
    }

    @Override
    public List<String> getNodes() {
        return gossipService.getMembers().entrySet().stream()
                .filter(e -> !e.getKey().equals(selfNode) && e.getValue().getState() != NodeState.DEAD)
                .map(java.util.Map.Entry::getKey)
                .toList();
    }

    @Override
    public ValueEntry read(String key, ValueEntry localValue) {
        List<String> preferenceList = ring.getPreferenceList(key, replicationFactor);
        if (!preferenceList.contains(selfNode)) {
            return fetchFromPreferenceList(key, preferenceList);
        }
        CompletableFuture.runAsync(() -> detectAndRepair(key, localValue));
        return localValue;
    }

    private ValueEntry fetchFromPreferenceList(String key, List<String> preferenceList) {
        for (String node : preferenceList) {
            try {
                return restTemplate.getForObject("http://" + node + "/kv/internal/get?key=" + key, ValueEntry.class);
            } catch (Exception e) {
                log.debug("Forward read for key={} to {} failed: {}", key, node, e.getMessage());
            }
        }
        return null;
    }

    private void detectAndRepair(String key, ValueEntry localValue) {
        List<String> preferenceList = ring.getPreferenceList(key, replicationFactor);
        List<NodeResponse> responses = new ArrayList<>();

        // Only count self if this node owns the key.
        if (preferenceList.contains(selfNode)) {
            responses.add(new NodeResponse(selfNode, localValue));
        }

        List<CompletableFuture<NodeResponse>> futures = new ArrayList<>();
        for (String node : preferenceList.stream().filter(n -> !n.equals(selfNode)).toList()) {
            futures.add(CompletableFuture.supplyAsync(() -> {
                try {
                    ValueEntry entry = restTemplate.getForObject(
                            "http://" + node + "/kv/internal/get?key=" + key, ValueEntry.class);
                    return new NodeResponse(node, entry);
                } catch (Exception e) {
                    return null;
                }
            }));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .orTimeout(1000, java.util.concurrent.TimeUnit.MILLISECONDS)
                .exceptionally(e -> null)
                .join();
        futures.stream().map(f -> f.getNow(null)).filter(r -> r != null).forEach(responses::add);

        List<ValueEntry> validEntries = responses.stream()
                .map(NodeResponse::getEntry)
                .filter(e -> e != null)
                .toList();

        if (validEntries.isEmpty()) return;

        ValueEntry latest = validEntries.stream()
                .max((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()))
                .orElse(null);

        if (latest != null) repairStaleNodes(key, latest, responses);
    }

    private void repairStaleNodes(String key, ValueEntry latest, List<NodeResponse> responses) {
        for (NodeResponse res : responses) {
            ValueEntry entry = res.getEntry();
            if (entry == null || entry.getTimestamp() < latest.getTimestamp()) {
                if (res.getNode().equals(selfNode)) {
                    keyValService.putInternal(key, latest.getValue(), latest.getTimestamp(), latest.isDeleted());
                } else {
                    asyncSender.send(res.getNode(),
                            new ReplicationRequest(key, latest.getValue(), latest.getTimestamp(), latest.isDeleted()));
                }
            }
        }
    }
}
