package com.aazim.kvstore.model;

public class ReplicationRequest {
    private String key;
    private String value;
    private long ts;
    private boolean deleted;

    public ReplicationRequest() {}

    public ReplicationRequest(String key, String value, long ts) {
        this.key = key;
        this.value = value;
        this.ts = ts;
    }

    public ReplicationRequest(String key, String value, long ts, boolean deleted) {
        this.key = key;
        this.value = value;
        this.ts = ts;
        this.deleted = deleted;
    }

    public String getKey()              { return key; }
    public void setKey(String key)      { this.key = key; }

    public String getValue()            { return value; }
    public void setValue(String value)  { this.value = value; }

    public long getTimestamp()          { return ts; }
    public void setTimestamp(long ts)   { this.ts = ts; }

    public boolean isDeleted()              { return deleted; }
    public void setDeleted(boolean deleted) { this.deleted = deleted; }
}