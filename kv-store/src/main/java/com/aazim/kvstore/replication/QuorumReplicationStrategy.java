package com.aazim.kvstore.replication;

import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Value;
import com.aazim.kvstore.model.ValueEntry;

@Component
public class QuorumReplicationStrategy implements ReplicationStrategy {
    private final RestTemplate restTemplate = new RestTemplate();

    //followers
    private final List<String> nodes  = List.of(
        "http://localhost:8081/kv/internal/replicate",
        "http://localhost:8082/kv/internal/replicate"
    );
    @Value("${write.quorum:2}")
    private int writeQuorum; // default to 2 if not set

    @Override
    public boolean handleWrite(String key, String value, long timestamp){
        int successCount = 0;
        for (String node: nodes){
            try {
                Boolean response = restTemplate.postForObject(node+"?key={k}&value={v}&ts={t}", null,Boolean.class, key, value, timestamp);
                System.out.println("Replicated to " + node + ": " + key + "=" + value);

                if (Boolean.TRUE.equals(response)) {
                    successCount++;
                }
            } catch (Exception e) {
                System.err.println("Failed to replicate to " + node + ": " + e.getMessage());
            }
        }
        System.out.println("Write quorum: " + successCount + "/" + writeQuorum);
        return successCount >= writeQuorum; // Write is successful if we meet the quorum
    }
    @Override
    public List<String> getNodes() {
        return nodes;
    }

    @Override
    public ValueEntry fetchFromNode(String node, String key) {
        try {
            return restTemplate.getForObject(node.replace("/replicate", "/get")+"?key={k}", ValueEntry.class, key);
        } catch (Exception e) {
            return null; 
        }
    }

    
}
