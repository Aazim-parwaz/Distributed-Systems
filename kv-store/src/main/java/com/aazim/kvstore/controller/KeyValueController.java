package com.aazim.kvstore.controller;

import com.aazim.kvstore.service.KeyValService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kv")
public class KeyValueController {
    private final KeyValService service;

    public KeyValueController(KeyValService service) {
        this.service = service;
    }

    @PutMapping
    public String put(@RequestParam String key, @RequestParam String value) {
        service.put(key, value);
        return "OK";
    }

    @GetMapping
    public String get(@RequestParam String key) {
        String value = service.get(key);
        return value != null ? value : "Key not found";
    }
}
