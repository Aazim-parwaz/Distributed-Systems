package com.aazim.kvstore.replication;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import com.aazim.kvstore.model.ValueEntry;


@Component
public class AsyncReplicationStrategy implements ReplicationStrategy {
    private final RestTemplate restTemplate = new RestTemplate();

    //followers
    @Value("${Nodes}")
    private String nodesConfig;
    
    // private final List<String> nodes  = List.of(
    //     "http://localhost:8081/kv/internal/replicate",
    //     "http://localhost:8082/kv/internal/replicate"
    // );

    @Override
    public boolean replicate(String key, String value, long timestamp){
        List<String> nodes = Arrays.asList(nodesConfig.split(","));
        for (String node: nodes){
            node = "http://"+node+"/kv/internal/replicate";
            sendAsync(node,key,value,timestamp);
        }

        return true;
    }

    @Async
    public void sendAsync(String node, String key, String value, long timestamp){
        try {
            restTemplate.postForObject(node+"?key={k}&value={v}&ts={t}", null, String.class, key, value, timestamp);
            System.out.println("Replicated to " + node + ": " + key + "=" + value);
        } catch (Exception e) {
            System.err.println("Failed to replicate to " + node + ": " + e.getMessage());
            // Optionally implement retry logic here
        }
    }

    @Override
    public List<String> getNodes() {
        List<String> nodes = Arrays.asList(nodesConfig.split(","));
        return nodes;
    }
    // i don't have any /internal/get api point- need to fix this also.
    @Override
    public ValueEntry fetchFromNode(String node, String key) {
        try {
            return restTemplate.getForObject(node+"/internal/get?key={k}", ValueEntry.class, key);
        } catch (Exception e) {
            return null; 
        }
    }
    
}
