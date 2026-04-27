package com.aazim.kvstore.replication;

import com.aazim.kvstore.model.ValueEntry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class MerkleTree {

    public static final int BUCKET_COUNT = 16; // must be power of 2

    private final String[] hashes;  // flat array: 2*BUCKET_COUNT - 1 nodes, root at 0
    private final Map<Integer, Map<String, ValueEntry>> buckets;

    public MerkleTree(Map<String, ValueEntry> data) {
        this.buckets = new HashMap<>();
        this.hashes = new String[2 * BUCKET_COUNT - 1];

        for (Map.Entry<String, ValueEntry> e : data.entrySet()) {
            int b = bucketFor(e.getKey());
            buckets.computeIfAbsent(b, k -> new TreeMap<>()).put(e.getKey(), e.getValue());
        }

        buildTree();
    }

    private void buildTree() {
        for (int b = 0; b < BUCKET_COUNT; b++) {
            hashes[BUCKET_COUNT - 1 + b] = hashBucket(buckets.getOrDefault(b, Collections.emptyMap()));
        }
        for (int i = BUCKET_COUNT - 2; i >= 0; i--) {
            hashes[i] = sha256(hashes[2 * i + 1] + hashes[2 * i + 2]);
        }
    }

    private String hashBucket(Map<String, ValueEntry> entries) {
        if (entries.isEmpty()) return sha256("empty");
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, ValueEntry> e : entries.entrySet()) {
            sb.append(e.getKey()).append(':')
              .append(e.getValue().getValue()).append(':')
              .append(e.getValue().getTimestamp()).append(';');
        }
        return sha256(sb.toString());
    }

    private static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : bytes) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public String getHash(int nodeIndex) {
        return hashes[nodeIndex];
    }

    public boolean isLeaf(int nodeIndex) {
        return nodeIndex >= BUCKET_COUNT - 1;
    }

    public int leftChild(int nodeIndex) {
        return 2 * nodeIndex + 1;
    }

    public int rightChild(int nodeIndex) {
        return 2 * nodeIndex + 2;
    }

    public int bucketIndexForLeaf(int leafNodeIndex) {
        return leafNodeIndex - (BUCKET_COUNT - 1);
    }

    public Map<String, ValueEntry> getBucketEntries(int bucketIndex) {
        return Collections.unmodifiableMap(buckets.getOrDefault(bucketIndex, Collections.emptyMap()));
    }

    public static int bucketFor(String key) {
        return Math.abs(key.hashCode()) % BUCKET_COUNT;
    }
}
