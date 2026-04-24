package com.aazim.kvstore.replication;

import java.util.Arrays;
import java.util.List;

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
        return localValue;
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
