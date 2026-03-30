package com.aazim.kvstore.replication;


import org.springframework.stereotype.Component;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.Key;
import java.util.List;

@Component
public class ReplicationManager {
    private final List<String> followers  = List.of(
            "http://localhost:8081/kv",
            "http://localhost:8082/kv"
    );
    private final BlockingQueue<ReplicationTask> replicationQueue = new LinkedBlockingQueue<>();

    public ReplicationManager() {
        // Start a background thread to process replication tasks
        startWorker();
    }
    // called by leader (Non-blocking)
    public void replicate(String key, String value){
        System.out.println("Queued: " + key + "=" + value);
        replicationQueue.offer(new ReplicationTask(key, value));
    }
    // Background worker thread
    private void startWorker() {
        Thread worker = new Thread(() -> {
            while (true) {
                try {
                    ReplicationTask task = replicationQueue.take(); // Blocking call

                    String key = task.key;
                    String value = task.value;

                    System.out.println("Replicating: " + key + "=" + value);

                    sendToFollowers(task);


                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                    break; // Exit on interrupt
                }
            }
        });
        worker.setDaemon(true); // Don't block JVM shutdown(JVM exits cleanly)
        worker.start();
    }

    // Actual HTTP replication + retry
    private void sendToFollowers(ReplicationTask task) {
        for(String follower : followers){
            try {
                URL url = new URL(follower + "?key=" + task.key + "&value=" + task.value);
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();

                conn.setRequestMethod("PUT");
                conn.setDoOutput(true);

                int responseCode =conn.getResponseCode(); // Trigger the request
                if (responseCode != 200) {
                    throw new RuntimeException("Failed to replicate to " + follower + ", response code: " + responseCode);
                }

            } catch (Exception e) {
                if (task.retries < 3) {
                    task.retries++;
                    System.out.println("Failed to replicate to " + follower + ", retry " + task.retries);
                    try {
                        Thread.sleep(1000 * task.retries); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                    // Retry layer (re-queue)
                replicationQueue.offer(task);
                } else{
                    System.out.println("Failed to replicate to " + follower + " after 3 retries, giving up." + task.key);
                }
                
            }
        }
    }
}
