package com.aazim.kvstore.replication;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.aazim.kvstore.model.Hint;

@Component
public class HintStore {

    private static final Logger log = LoggerFactory.getLogger(HintStore.class);
    private static final int MAX_HINTS_PER_NODE = 500;
    private static final long MAX_HINT_AGE_MS   = 3_600_000; // 1 hour

    @Value("${node.id}")
    private String nodeId;

    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Hint>> pending = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> fileLocks = new ConcurrentHashMap<>();

    @PostConstruct
    public void loadHints() {
        File[] hintFiles = new File(".").listFiles(
                (dir, name) -> name.startsWith("hints_" + nodeId + "_") && name.endsWith(".log"));
        if (hintFiles == null) return;

        for (File file : hintFiles) {
            String targetNode = fileNameToNode(file.getName());
            if (targetNode == null) continue;
            List<Hint> loaded = readFile(file);
            if (!loaded.isEmpty()) {
                pending.computeIfAbsent(targetNode, k -> new ConcurrentLinkedQueue<>()).addAll(loaded);
                log.info("Recovered {} hints for {} from disk", loaded.size(), targetNode);
            }
        }
    }

    public void store(String node, Hint hint) {
        ConcurrentLinkedQueue<Hint> queue = pending.computeIfAbsent(node, k -> new ConcurrentLinkedQueue<>());
        if (queue.size() >= MAX_HINTS_PER_NODE) {
            log.warn("Queue full for {}, dropping hint for key={}", node, hint.getKey());
            return;
        }
        queue.add(hint);
        appendToFile(node, hint);
        log.info("Stored hint for {} key={} (queued={})", node, hint.getKey(), queue.size());
    }

    public List<Hint> drain(String node) {
        ConcurrentLinkedQueue<Hint> queue = pending.remove(node);
        if (queue == null) return Collections.emptyList();

        long cutoff = System.currentTimeMillis() - MAX_HINT_AGE_MS;
        List<Hint> valid = queue.stream()
                .filter(h -> h.getCreatedAt() > cutoff)
                .collect(Collectors.toList());

        int dropped = queue.size() - valid.size();
        if (dropped > 0) log.warn("Dropped {} expired hints for {}", dropped, node);

        // File cleared here; restore() rewrites it for any undelivered hints.
        deleteFile(node);
        return valid;
    }

    public void restore(String node, List<Hint> hints) {
        hints.forEach(h -> store(node, h));
    }

    public Set<String> nodesWithHints() {
        return new HashSet<>(pending.keySet());
    }

    public Map<String, Integer> counts() {
        Map<String, Integer> result = new HashMap<>();
        pending.forEach((node, queue) -> result.put(node, queue.size()));
        return result;
    }

    // ── file helpers ──────────────────────────────────────────────────────────

    private void appendToFile(String node, Hint hint) {
        synchronized (fileLock(node)) {
            try (BufferedWriter w = new BufferedWriter(new FileWriter(hintFile(node), true))) {
                w.write(hint.getKey() + ","
                        + (hint.getValue() != null ? hint.getValue() : "") + ","
                        + hint.getTimestamp() + ","
                        + hint.isDeleted() + ","
                        + hint.getCreatedAt());
                w.newLine();
            } catch (IOException e) {
                log.error("Failed to persist hint for {}: {}", node, e.getMessage());
            }
        }
    }

    private void deleteFile(String node) {
        synchronized (fileLock(node)) {
            File f = hintFile(node);
            if (f.exists()) f.delete();
        }
    }

    private List<Hint> readFile(File file) {
        List<Hint> hints = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = r.readLine()) != null) {
                String[] p = line.split(",", 5);
                if (p.length == 5) {
                    hints.add(new Hint(p[0], p[1].isEmpty() ? null : p[1],
                            Long.parseLong(p[2]), Boolean.parseBoolean(p[3]),
                            Long.parseLong(p[4])));
                }
            }
        } catch (IOException e) {
            log.warn("Could not read hint file {}: {}", file.getName(), e.getMessage());
        }
        return hints;
    }

    private File hintFile(String node) {
        return new File("hints_" + nodeId + "_" + node.replace(":", "_") + ".log");
    }

    private String fileNameToNode(String fileName) {
        // hints_<nodeId>_<host>_<port>.log → <host>:<port>
        String prefix = "hints_" + nodeId + "_";
        if (!fileName.startsWith(prefix) || !fileName.endsWith(".log")) return null;
        String encoded = fileName.substring(prefix.length(), fileName.length() - 4);
        int lastUnderscore = encoded.lastIndexOf('_');
        if (lastUnderscore < 0) return null;
        return encoded.substring(0, lastUnderscore) + ":" + encoded.substring(lastUnderscore + 1);
    }

    private Object fileLock(String node) {
        return fileLocks.computeIfAbsent(node, k -> new Object());
    }
}
