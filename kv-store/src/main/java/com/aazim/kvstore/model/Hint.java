package com.aazim.kvstore.model;

public class Hint {
    private final String key;
    private final String value;
    private final long timestamp;
    private final long createdAt;

    public Hint(String key, String value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.createdAt = System.currentTimeMillis();
    }

    public String getKey()       { return key; }
    public String getValue()     { return value; }
    public long getTimestamp()   { return timestamp; }
    public long getCreatedAt()   { return createdAt; }
}
