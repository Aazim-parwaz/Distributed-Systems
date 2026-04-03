package com.aazim.kvstore.replication;

public interface ReplicationStrategy {
    void replicate(String key, String value);
}
