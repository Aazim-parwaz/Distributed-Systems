package com.aazim.kvstore.storage;

import org.springframework.stereotype.Component;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


@Component
public class LogStore {

    private static final String LOG_FILE = "kvstore.log";

    //synchronisation is thread safe
    public synchronized void append(String key, String value) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
            writer.write(key + "=" + value);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to log file", e);
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