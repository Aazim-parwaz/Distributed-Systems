package com.aazim.kvstore.replication;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AsyncReplicationStrategy implements ReplicationStrategy {

    @Autowired
    private ReplicationManager replicationManager;

    @Override
    public void replicate(String key, String value) {
        replicationManager.replicate(key, value); // queue + retry
    }
    
}
