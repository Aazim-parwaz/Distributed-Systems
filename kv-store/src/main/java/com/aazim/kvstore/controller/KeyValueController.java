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
}
