package com.aazim.kvstore.replication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ReplicationStrategyFactory {
    @Value("${replication.mode:async}")
    private String replicationMode;

    @Autowired
    private AsyncReplicationStrategy asyncStrategy;

    @Autowired
    private QuorumReplicationStrategy quorumStrategy;

    public ReplicationStrategy getStrategy() {
        System.out.println("Replication mode: " +replicationMode);
        if ("quorum".equalsIgnoreCase(replicationMode)) {
            return quorumStrategy;
        }
        
        return asyncStrategy; // default
        
    }
}
