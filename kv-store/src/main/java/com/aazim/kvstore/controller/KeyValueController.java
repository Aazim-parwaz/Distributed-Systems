package com.aazim.kvstore.controller;

import java.util.Map;

import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.replication.HintStore;
import com.aazim.kvstore.service.KeyValService;

import org.apache.catalina.connector.Response;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.aazim.kvstore.model.ReplicationRequest;

@RestController
@RequestMapping("/kv")
public class KeyValueController {
    private final KeyValService service;
    private final HintStore hintStore;

    public KeyValueController(KeyValService service, HintStore hintStore) {
        this.service = service;
        this.hintStore = hintStore;
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
}
