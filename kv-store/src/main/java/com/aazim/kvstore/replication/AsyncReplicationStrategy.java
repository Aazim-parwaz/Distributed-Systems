package com.aazim.kvstore.replication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.aazim.kvstore.model.ReplicationRequest;
import com.aazim.kvstore.model.ValueEntry;

@Component
public class AsyncReplicationStrategy implements ReplicationStrategy {

    private final RestTemplate restTemplate;
    private final WebServerApplicationContext context;

    @Value("${Nodes}")
    private String nodesConfig;

    private String selfNode;

    public AsyncReplicationStrategy(RestTemplate restTemplate, WebServerApplicationContext context) {
        this.restTemplate = restTemplate;
        this.context = context;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        selfNode = "localhost:" + context.getWebServer().getPort();
    }

    @Override
    public boolean replicate(String key, String value, long timestamp) {
        for (String node : getNodes()) {
            sendAsync("http://" + node + "/kv/internal/replicate", key, value, timestamp);
        }
        return true;
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
        List<NodeResponse> responses = new ArrayList<>();
        responses.add(new NodeResponse(selfNode, localValue));

        List<CompletableFuture<NodeResponse>> futures = new ArrayList<>();
        for (String node : getNodes()) {
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
        futures.stream().map(f -> f.getNow(null)).filter(r -> r != null).forEach(responses::add);

        List<ValueEntry> validEntries = responses.stream()
                .map(NodeResponse::getEntry)
                .filter(e -> e != null)
                .toList();

        if (validEntries.isEmpty()) {
            return localValue;
        }

        ValueEntry latest = validEntries.stream()
                .max((a, b) -> Long.compare(a.getTimestamp(), b.getTimestamp()))
                .orElse(localValue);

        CompletableFuture.runAsync(() -> repairStaleNodes(key, latest, responses));

        return latest;
    }

    private void repairStaleNodes(String key, ValueEntry latest, List<NodeResponse> responses) {
        for (NodeResponse res : responses) {
            ValueEntry entry = res.getEntry();
            if (entry == null || entry.getTimestamp() < latest.getTimestamp()) {
                sendAsync("http://" + res.getNode() + "/kv/internal/replicate",
                          key, latest.getValue(), latest.getTimestamp());
            }
        }
    }

    @Async
    public void sendAsync(String url, String key, String value, long timestamp) {
        try {
            restTemplate.postForObject(url, new ReplicationRequest(key, value, timestamp), Boolean.class);
            System.out.println("Replicated to " + url + ": " + key + "=" + value);
        } catch (Exception e) {
            System.err.println("Failed to replicate to " + url + ": " + e.getMessage());
        }
    }
}
