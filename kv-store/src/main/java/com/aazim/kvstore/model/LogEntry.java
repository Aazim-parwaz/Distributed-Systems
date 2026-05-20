package com.aazim.kvstore.model;

public class LogEntry {
    private String key;
    private String value;
    private long timestamp;
    private boolean deleted;

    public LogEntry(String key, String value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public LogEntry(String key, String value, long timestamp, boolean deleted) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.deleted = deleted;
    }

    public String getKey()      { return key; }
    public String getValue()    { return value; }
    public long getTimestamp()  { return timestamp; }
    public boolean isDeleted()  { return deleted; }
}
