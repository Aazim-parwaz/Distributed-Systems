package com.aazim.kvstore.controller;

import java.util.List;
import java.util.Map;

import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.replication.ConsistentHashRing;
import com.aazim.kvstore.replication.HintStore;
import com.aazim.kvstore.replication.MerkleTree;
import com.aazim.kvstore.service.KeyValService;

import org.apache.catalina.connector.Response;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.aazim.kvstore.model.ReplicationRequest;

@RestController
@RequestMapping("/kv")
public class KeyValueController {
    private final KeyValService service;
    private final HintStore hintStore;
    private final ConsistentHashRing ring;

    @Value("${replication.factor:3}")
    private int replicationFactor;

    public KeyValueController(KeyValService service, HintStore hintStore, ConsistentHashRing ring) {
        this.service = service;
        this.hintStore = hintStore;
        this.ring = ring;
    }

    @PutMapping("/put")
    public String put(@RequestParam String key, @RequestParam String value) {
        return service.put(key, value) ? "SUCCESS" : "FAILURE";
    }

    @GetMapping("/get")
    public ResponseEntity<String> get(@RequestParam String key) {
        ValueEntry entry = service.read(key);
        if (entry != null) {
            return ResponseEntity.ok(entry.getValue());
        } else {
            return ResponseEntity.status(Response.SC_NOT_FOUND).body("Key not found");
        }
    }

    // Follower write path — coordinator has already assigned the timestamp.
    // putInternal handles last-write-wins internally; no need to re-check here.
    @PostMapping("/internal/replicate")
    public boolean replicate(@RequestBody ReplicationRequest request) {
        service.putInternal(request.getKey(), request.getValue(), request.getTimestamp());
        return true;
    }

    // Internal read used by other nodes for quorum reads and read-repair.
    @GetMapping("/internal/get")
    public ValueEntry read(@RequestParam String key) {
        return service.get(key);
    }

    // Debug: pending hint counts per node on this coordinator.
    @GetMapping("/hints")
    public Map<String, Integer> hints() {
        return hintStore.counts();
    }

    // Debug: which nodes own a given key according to the ring.
    @GetMapping("/ring")
    public List<String> ring(@RequestParam String key) {
        return ring.getPreferenceList(key, replicationFactor);
    }

    // Anti-entropy: return all 2*BUCKET_COUNT-1 hashes in one shot so the
    // initiator can diff the entire tree locally without further round-trips.
    @GetMapping("/internal/merkle/tree")
    public String[] merkleTree() {
        return new MerkleTree(service.getAll()).getAllHashes();
    }

    // Anti-entropy: return all key-value entries in the given bucket.
    @GetMapping("/internal/merkle/bucket/{bucketIndex}")
    public Map<String, ValueEntry> merkleBucket(@PathVariable int bucketIndex) {
        return new MerkleTree(service.getAll()).getBucketEntries(bucketIndex);
    }

    // Debug: hash at a specific node index (kept for manual inspection).
    @GetMapping("/internal/merkle/hash/{nodeIndex}")
    public String merkleHash(@PathVariable int nodeIndex) {
        return new MerkleTree(service.getAll()).getHash(nodeIndex);
    }
}
