package com.aazim.kvstore.replication;

public class ReplicationTask {
    public String key;
    public String value;
    public int retries;

    public ReplicationTask(String key, String value) {
        this.key = key;
        this.value = value;
        this.retries = 0;
    }
}
