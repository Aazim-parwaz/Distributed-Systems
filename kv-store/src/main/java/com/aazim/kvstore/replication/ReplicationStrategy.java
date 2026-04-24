package com.aazim.kvstore.replication;

import java.util.List;

import com.aazim.kvstore.model.ValueEntry;
// strategy interface for replication and quorum reads
public interface ReplicationStrategy {
    boolean replicate(String key, String value, long timestamp);
    List<String> getNodes();
    // ValueEntry fetchFromNode(String node, String key);
    // strategy-based read --> why localValue is needed here ? to compare with remote value and decide which one to return based on timestamp (last write wins)
    ValueEntry read(String key, ValueEntry localValue);
}
