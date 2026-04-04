package com.aazim.kvstore.replication;

import java.util.List;

import com.aazim.kvstore.model.ValueEntry;

public interface ReplicationStrategy {
    boolean handleWrite(String key, String value, long timestamp);
    List<String> getNodes();
    ValueEntry fetchFromNode(String node, String key);
}
