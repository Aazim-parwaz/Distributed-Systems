package com.aazim.kvstore.storage;

import com.aazim.kvstore.model.ValueEntry;

import lombok.extern.java.Log;

import com.aazim.kvstore.model.LogEntry;
import org.springframework.stereotype.Component;


import java.io.*;
import java.util.*;


@Component
public class FileLogStore implements LogStore{

    private final File LOG_FILE = new File("kvstore.log");
    private final Object writeLock  = new Object();

    
    
    @Override
    public void append(String key, String value, long timestamp) {
        synchronized (writeLock) {
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
                writer.write(key + "," + value + "," + timestamp);
                writer.newLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public List<LogEntry> readAll() {
        List<LogEntry> entries = new ArrayList<>();

        if(!LOG_FILE.exists()){
            return entries; // No log file, return empty list
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",", 3);
                if (parts.length == 3) {
                    String key = parts[0];
                    String value = parts[1];
                    long timestamp = Long.parseLong(parts[2]);
                    entries.add(new LogEntry(key, value, timestamp));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read log file", e);
        }
        return entries;
    }

    @Override
    public void compact(Map<String, ValueEntry> latestState) {
        File tempFile = new File("kvstore_temp.log");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            for (Map.Entry<String, ValueEntry> entry : latestState.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue().getValue() + "," + entry.getValue().getTimestamp());
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write compacted log file", e);
        }
        synchronized (writeLock) {
            
            if (!tempFile.renameTo(LOG_FILE)) {
                throw new RuntimeException("Failed to replace log file with compacted version");
            }
            LOG_FILE.delete(); // Delete old log file
        }
    }
    public synchronized void compact(Map<String, String> latestData) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE))) {
            for (Map.Entry<String, String> entry : latestData.entrySet()) {
                writer.write(entry.getKey() + "=" + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to compact log file", e);
        }
    }
    // Replay Log
    public Map<String,String> load(){
        Map<String,String> data = new HashMap<>();
        File file = new File(LOG_FILE);
        if(!file.exists()){
            return data; // No log file, return empty map
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(LOG_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("=", 2);
                if (parts.length == 2) {
                    data.put(parts[0], parts[1]); //last write wins
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read log file", e);
        }
        return data;
    }
}