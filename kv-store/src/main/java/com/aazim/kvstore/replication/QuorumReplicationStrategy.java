package com.aazim.kvstore.replication;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.event.EventListener;

import com.aazim.kvstore.model.ValueEntry;


@Component
public class QuorumReplicationStrategy implements ReplicationStrategy {
    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${write.quorum:2}")
    private int writeQuorum; // default to 2 if not set

    @Value("${Nodes}")
    private String nodesConfig;

    @Autowired
    private WebServerApplicationContext context;

    private String selfNode;

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        int port = context.getWebServer().getPort();
        selfNode = "localhost:" + port; 
        System.out.println("Node started on port: " + port);

    }

    @Override
    public boolean replicate(String key, String value, long timestamp){
        List<String> nodes = Arrays.stream(nodesConfig.split(","))
                                                        .map(String::trim)
                                                        .filter(node -> !node.equals(selfNode)) // IMPORTANT
                                                        .toList();

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
        System.out.println("Nodes for read: " + nodes);

        int totalNodes = nodes.size() + 1; // including self
        int readQuorum = (totalNodes / 2) + 1; // majority for read

        // List<ValueEntry> responses = new ArrayList<>();
        // //local read
        // if (localValue != null) {
        //     responses.add(localValue);
        // }

        List<CompletableFuture<NodeResponse>> futures = new ArrayList<>();

        futures.add(CompletableFuture.completedFuture(new NodeResponse(selfNode, localValue)));
        System.out.println("Self Node: "+selfNode);


        //remote read
        for (String node: nodes){
            CompletableFuture<NodeResponse> future = CompletableFuture.supplyAsync(() -> {
                try {
                    System.out.println("Fetching from " + node + " for key: " + key);
                    ValueEntry entry = restTemplate.getForObject("http://"+node+"/kv/internal/get?key="+key, ValueEntry.class);
                    return new NodeResponse(node, entry);
                } catch (Exception e) {
                    System.err.println("Failed to read from " + node + ": " + e.getMessage());
                    return new NodeResponse(node, null); // Return null on failure
                }
            });
            futures.add(future);
        }

        List<NodeResponse> responses = new ArrayList<>();

        // Early quorum exit: Wait for responses and check if we have enough to meet the quorum before waiting for all

        for (CompletableFuture<NodeResponse> future : futures) {
            try {
                NodeResponse res = future.get(500,TimeUnit.MILLISECONDS); // Wait for each read to complete
                responses.add(res);
                if(responses.size() >= readQuorum){
                    break; // We have enough responses to meet the quorum, no need to wait for more
                }
            } catch (Exception e) {
                System.err.println("Error while waiting for read response: " + e.getMessage());
            }
        }
        
        // Same quorum check after waiting for responses, in case we didn't meet it during the early exit
        if (responses.size() < readQuorum) {
            throw new RuntimeException("Read quorum not met. Available replicas: " + responses.size() + "/" + readQuorum); 
        }

        List<ValueEntry> validEntries = responses.stream().map(NodeResponse::getEntry).filter(e -> e != null).toList();

        //Last write wins
        ValueEntry latest = getLatest(validEntries);

        if (latest == null) {
            return null; // No valid entries found
        }
        
        // Make repari async (non-blocking)
        CompletableFuture.runAsync(() -> repairNodes(key, latest, responses));

        return latest;
    }

    private ValueEntry getLatest(List<ValueEntry> responses){
        return responses.stream().filter(e -> e != null).max((e1,e2) -> Long.compare(e1.getTimestamp(), e2.getTimestamp())).orElse(null);
    }

    

    private void repairNodes(String key,ValueEntry latest, List<NodeResponse> responses){
        System.out.println("Starting read repair for key: " + key);
        for (NodeResponse res: responses){
            System.out.println("checkpoint 1");
            
            String node = res.getNode();
            System.out.println("checking node: " + node);   
            System.out.println(node + "then "+ selfNode);
            if (node!=null && node.equals(selfNode)){
                System.out.println("self skipped");
                continue; // Skip self
            }
            ValueEntry entry = res.getEntry();
            if (entry == null || entry.getTimestamp() < latest.getTimestamp()){
                try {
                    System.out.println("checkpoint 2");
                    System.out.println("Repairing node " + node + " for key: " + key + ". Latest value: " + latest.getValue() + ", Node value: " + (entry != null ? entry.getValue() : "null"));
                    String url = "http://" + node + "/kv/internal/replicate";
                    System.out.println(url);
                    Map<String, Object> request = new HashMap<>();

                    request.put("key", key);
                    request.put("value", latest.getValue());
                    request.put("ts", latest.getTimestamp());

                    restTemplate.postForObject(url,request, Boolean.class);
                    System.out.println("Sent read repair to " + res.getNode() + ": " + key + "=" + latest.getValue());
                } catch (Exception e) {
                    System.err.println("Failed to send read repair to " + res.getNode() + ": " + e.getMessage());
                }
            }
            System.out.println("checkpoint 3");
            
        }
    }

    
}
