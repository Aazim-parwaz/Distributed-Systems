package com.aazim.kvstore.replication;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Value;
import com.aazim.kvstore.model.ValueEntry;

@Component
public class QuorumReplicationStrategy implements ReplicationStrategy {
    private final RestTemplate restTemplate = new RestTemplate();

    //followers
    
    @Value("${write.quorum:2}")
    private int writeQuorum; // default to 2 if not set

    @Value("${Nodes}")
    private String nodesConfig;

    // private final List<String> nodes  = List.of(
    //     "http://localhost:8081/kv/internal/replicate",
    //     "http://localhost:8082/kv/internal/replicate"
    // );

    @Override
    public boolean handleWrite(String key, String value, long timestamp){
        List<String> nodes = Arrays.asList(nodesConfig.split(","));

        int totalNodes = nodes.size() + 1; // including self
        int majority = (totalNodes / 2) + 1;

        int effectiveQuorum = Math.min(writeQuorum, totalNodes); // Ensure we don't require more than majority

        if (effectiveQuorum < majority) {
            System.err.println("Write quorum of " + effectiveQuorum + " is too low for total nodes " + totalNodes + ". Adjusting to majority: " + majority);
            effectiveQuorum = majority; // Adjust to majority if configured quorum is too low
        }
        int successCount = 1;

        System.out.println("Quorum required: " + effectiveQuorum);

        for (String node: nodes){
            try {
                Boolean response = restTemplate.postForObject("http://"+node+"/kv/internal/replicate?key="+key
                    +"&value="+value+"&ts="+timestamp, null, Boolean.class);
                System.out.println("Replicated to " + node + ": " + key + "=" + value);

                if (Boolean.TRUE.equals(response)) {
                    successCount++;
                    System.out.println("Replication successful for " + node);

                    if (successCount >= effectiveQuorum) {
                        System.out.println("Write quorum achieved with " + successCount + "/" + effectiveQuorum);
                        return true; // We have enough successful writes, no need to continue
                    }
                }
            } catch (Exception e) {
                System.err.println("Failed to replicate to " + node + ": " + e.getMessage());
            }
        }

        System.out.println("Write quorum: " + successCount);
        return successCount >= effectiveQuorum; // Write is successful if we meet the quorum
    }
    @Override
    public List<String> getNodes() {
        List<String> nodes = Arrays.asList(nodesConfig.split(","));
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
