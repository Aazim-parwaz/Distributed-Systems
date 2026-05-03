package com.aazim.kvstore.replication;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

@Component
public class ConsistentHashRing {

    private static final Logger log = LoggerFactory.getLogger(ConsistentHashRing.class);
    private static final int VIRTUAL_NODES = 150;

    // Sorted map of hash position → physical node address.
    // TreeMap gives O(log N) clockwise lookup via tailMap().
    private final TreeMap<Long, String> ring = new TreeMap<>();

    @Value("${Nodes}")
    private String nodesConfig;

    @PostConstruct
    public void init() {
        Arrays.stream(nodesConfig.split(","))
              .map(String::trim)
              .filter(n -> !n.isEmpty())
              .forEach(this::addNode);

        long distinct = ring.values().stream().distinct().count();
        log.info("Ring initialised: {} physical nodes × {} vnodes = {} ring entries",
                 distinct, VIRTUAL_NODES, ring.size());
    }

    public synchronized void addNode(String node) {
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            ring.put(hash(node + "#" + i), node);
        }
    }

    public synchronized void removeNode(String node) {
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            ring.remove(hash(node + "#" + i));
        }
    }

    /**
     * Returns an ordered list of {@code count} distinct physical nodes responsible
     * for {@code key}, walking clockwise from the key's hash position on the ring.
     * The first node in the list is the primary (coordinator-preferred) owner.
     */
    public synchronized List<String> getPreferenceList(String key, int count) {
        if (ring.isEmpty()) return new ArrayList<>();
        long keyHash = hash(key);
        Set<String> result = new LinkedHashSet<>();

        for (String node : ring.tailMap(keyHash, true).values()) {
            result.add(node);
            if (result.size() == count) return new ArrayList<>(result);
        }
        // Wrap around to the start of the ring.
        for (String node : ring.values()) {
            result.add(node);
            if (result.size() == count) return new ArrayList<>(result);
        }
        return new ArrayList<>(result);
    }

    private long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] b = md.digest(input.getBytes(StandardCharsets.UTF_8));
            // Combine 4 bytes into an unsigned 32-bit value stored as long.
            return ((long) (b[3] & 0xFF) << 24)
                 | ((long) (b[2] & 0xFF) << 16)
                 | ((long) (b[1] & 0xFF) << 8)
                 | ((long) (b[0] & 0xFF));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
