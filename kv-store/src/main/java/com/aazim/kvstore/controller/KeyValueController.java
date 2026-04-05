package com.aazim.kvstore.controller;

import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.service.KeyValService;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kv")
public class KeyValueController {
    private final KeyValService service;

    public KeyValueController(KeyValService service) {
        this.service = service;
    }

    @PutMapping("/put")
    public String put(@RequestParam String key, @RequestParam String value) {
        return service.put(key, value) ? "SUCCESS" : "FAILURE";
    }

    @GetMapping("/get")
    public ValueEntry get(@RequestParam String key) {
        return service.get(key);
    }
    @PostMapping("/internal/replicate")
    public boolean replicate(@RequestParam String key, @RequestParam String value, @RequestParam long ts){
        ValueEntry existing = service.get(key);
        // System.out.println("Received replication for key: " + key + ", value: " + value + ", timestamp: " + ts);
        //last write wins
        if (existing ==null || existing.getTimestamp() < ts){

            System.out.println("Received replication for key: " + key + ", value: " + value + ", timestamp: " + ts);
            service.putInternal(key, value,ts);    
        }
        return true;
    }
}
