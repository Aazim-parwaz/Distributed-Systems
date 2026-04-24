package com.aazim.kvstore.storage;

import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.model.LogEntry; // Ensure LogEntry is imported
import java.util.List;
import java.util.Map;

public interface LogStore {
    void append(String key, String value, long timestamp);

    List<LogEntry> readAll();

    void compact(Map<String,ValueEntry> latestState);
}