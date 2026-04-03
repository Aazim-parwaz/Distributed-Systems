package com.aazim.kvstore.replication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.springframework.beans.factory.annotation.Value;

@Component
public class QuorumReplicationStrategy implements ReplicationStrategy {
    @Autowired
    private ReplicationManager replicationManager;

    @Value("${write.quorum:2}")
    private int writeQuorum; // default to 2 if not set

    @Override
    public void replicate(String key, String value) {
        int successCount = replicationManager.replicateAndCount(key, value);

        if (successCount < writeQuorum) {
            throw new RuntimeException("Failed!! Write quorum not met . Success count: " + successCount);
        }
    }
}
