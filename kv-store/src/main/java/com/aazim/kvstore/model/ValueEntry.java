package com.aazim.kvstore.model;

public class ValueEntry {
    private String value;
    private long timestamp;

    public ValueEntry() {}

    public ValueEntry(String value, long timestamp) {
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getValue() { return value; }

    public long getTimestamp() { return timestamp; }

    public void setValue(String value) { this.value = value; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    
}