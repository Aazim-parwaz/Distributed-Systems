package com.aazim.kvstore.model;

public class Hint {
    private final String key;
    private final String value;
    private final long timestamp;
    private final boolean deleted;
    private final long createdAt;

    public Hint(String key, String value, long timestamp) {
        this(key, value, timestamp, false);
    }

    public Hint(String key, String value, long timestamp, boolean deleted) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.deleted = deleted;
        this.createdAt = System.currentTimeMillis();
    }

    public String getKey()      { return key; }
    public String getValue()    { return value; }
    public long getTimestamp()  { return timestamp; }
    public boolean isDeleted()  { return deleted; }
    public long getCreatedAt()  { return createdAt; }
}
