package com.aazim.kvstore.replication;

import org.springframework.stereotype.Component;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

@Component
public class ReplicationManager {
    private final List<String> followers  = List.of(
            "http://localhost:8081/kv",
            "http://localhost:8082/kv"
    );
    public void replicate(String key, String value){
        for(String follower : followers){
            try {
                URL url = new URL(follower + "?key=" + key + "&value=" + value);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();

                conn.setRequestMethod("PUT");
                conn.setDoOutput(true);
                conn.getResponseCode(); // Trigger the request

            } catch (Exception e) {
                System.err.println("Error replicating to " + follower + ": " + e.getMessage());
            }
        }
    }
}
