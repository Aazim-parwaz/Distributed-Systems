package com.aazim.kvstore.replication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.aazim.kvstore.model.Hint;

@Component
public class HintStore {

    private static final Logger log = LoggerFactory.getLogger(HintStore.class);

    private static final int MAX_HINTS_PER_NODE = 500;
    private static final long MAX_HINT_AGE_MS   = 3_600_000; // 1 hour

    // node -> pending hints for that node
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Hint>> pending = new ConcurrentHashMap<>();

    public void store(String node, Hint hint) {
        // Enforce a max pending hint count per node to prevent unbounded memory growth.
        ConcurrentLinkedQueue<Hint> queue = pending.computeIfAbsent(node, k -> new ConcurrentLinkedQueue<>());
        if (queue.size() >= MAX_HINTS_PER_NODE) {
            log.warn("Queue full for {}, dropping hint for key={}", node, hint.getKey());
            return;
        }
        queue.add(hint);
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
        if (dropped > 0) {
            log.warn("Dropped {} expired hints for {}", dropped, node);
        }
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
}
