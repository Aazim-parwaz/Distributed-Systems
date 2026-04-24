package com.aazim.kvstore.replication;

import com.aazim.kvstore.model.ValueEntry;

public class NodeResponse {
    private String node;
    private ValueEntry entry;

    public NodeResponse(String node, ValueEntry entry) {
        this.node = node;
        this.entry = entry;
    }

    public String getNode() {
        return node;
    }

    public ValueEntry getEntry() {
        return entry;
    }

    
}
