package com.aazim.kvstore.replication;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Value;
import com.aazim.kvstore.model.ValueEntry;

@Component
public class QuorumReplicationStrategy implements ReplicationStrategy {
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${write.quorum:2}")
    private int writeQuorum; // default to 2 if not set

    @Value("${Nodes}")
    private String nodesConfig;

    @Override
    public boolean replicate(String key, String value, long timestamp){
        List<String> nodes = Arrays.stream(nodesConfig.split(",")).map(String::trim).toList();

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

                    // Early exit if we already have enough successful writes to meet the quorum(this works for quorum but not in async)
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
    public ValueEntry read(String key, ValueEntry localValue) {
        List<String> nodes = getNodes();

        int totalNodes = nodes.size() + 1; // including self
        int readQuorum = (totalNodes / 2) + 1; // majority for read

        List<ValueEntry> responses = new ArrayList<>();
        //local read
        if (localValue != null) {
            responses.add(localValue);
        }
        //remote read
        for (String node: nodes){
            try {
                ValueEntry entry = restTemplate.getForObject("http://"+node+"/kv/internal/get?key="+key, ValueEntry.class);
                if (entry != null) {
                    responses.add(entry);
                }
            } catch (Exception e) {
                System.err.println("Failed to read from " + node + ": " + e.getMessage());
            }
        }
        if (responses.size() < readQuorum) {
            throw new RuntimeException("Read quorum not met. Available replicas: " + responses.size() + "/" + readQuorum); 
        }

        //Last write wins
        ValueEntry latest = responses.stream().max((e1, e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp())).orElse(null);
        if (latest == null) {
            return null;
        }

        //Read repair
        for (String node: nodes){
            try {
                ValueEntry entry = restTemplate.getForObject("http://"+node+"/kv/internal/get?key="+key, ValueEntry.class);
                if (entry == null || entry.getTimestamp() < latest.getTimestamp()) {
                    // send repair
                    restTemplate.postForObject("http://"+node+"/kv/internal/replicate?key="+key
                        +"&value="+latest.getValue()+"&ts="+latest.getTimestamp(), null, String.class);
                    System.out.println("Sent read repair to " + node + ": " + key + "=" + latest.getValue());
                }
            } catch (Exception e) {
                System.err.println("Failed to send read repair to " + node + ": " + e.getMessage());
            }
        }
        return latest;
    }

    
}
