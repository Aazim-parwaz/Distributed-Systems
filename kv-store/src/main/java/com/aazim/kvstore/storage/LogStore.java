package com.aazim.kvstore.storage;

import org.springframework.stereotype.Component;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


@Component
public class LogStore {

    private static final String LOG_FILE = "kvstore.log";

    public synchronized void append(String key, String value) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(LOG_FILE, true))) {
            writer.write(key + "=" + value);
            writer.newLine();
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to log file", e);
        }
    }
}