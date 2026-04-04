package com.aazim.kvstore.replication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.springframework.beans.factory.annotation.Value;

@Component
public class QuorumReplicationStrategy implements ReplicationStrategy {
    

    @Value("${write.quorum:2}")
    private int writeQuorum; // default to 2 if not set

    
}
