package com.aazim.kvstore.model;

public class ValueEntry {
    private String value;
    private long timestamp;
    private boolean deleted;

    public ValueEntry() {}

    public ValueEntry(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public ValueEntry(String value, long timestamp, boolean deleted) {
        this.value = value;
        this.timestamp = timestamp;
        this.deleted = deleted;
    }

    public String getValue() { return value; }
    public long getTimestamp() { return timestamp; }
    public boolean isDeleted() { return deleted; }

    public void setValue(String value) { this.value = value; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public void setDeleted(boolean deleted) { this.deleted = deleted; }
}