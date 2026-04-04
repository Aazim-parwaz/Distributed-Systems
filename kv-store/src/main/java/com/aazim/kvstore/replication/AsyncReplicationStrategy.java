package com.aazim.kvstore.replication;

import java.util.List;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import com.aazim.kvstore.model.ValueEntry;


@Component
public class AsyncReplicationStrategy implements ReplicationStrategy {
    private final RestTemplate restTemplate = new RestTemplate();

    //followers
    private final List<String> nodes  = List.of(
        "http://localhost:8081/kv/internal/replicate",
        "http://localhost:8082/kv/internal/replicate"
    );

    @Override
    public boolean handleWrite(String key, String value, long timestamp){
        for (String node: nodes){

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
        return nodes;
    }

    @Override
    public ValueEntry fetchFromNode(String node, String key) {
        try {
            return restTemplate.getForObject(node+"/internal/get?key={k}", ValueEntry.class, key);
        } catch (Exception e) {
            return null; 
        }
    }
    
}
