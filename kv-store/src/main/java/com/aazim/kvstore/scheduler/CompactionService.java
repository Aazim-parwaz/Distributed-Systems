package com.aazim.kvstore.scheduler;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.aazim.kvstore.service.KeyValService;
import com.aazim.kvstore.storage.LogStore;

// design hint here. here this service uses two services.
@Component
public class CompactionService {
    private final LogStore logStore;
    private final KeyValService keyValueService;

    public CompactionService(LogStore logStore, KeyValService keyValueService) {
        this.logStore = logStore;
        this.keyValueService = keyValueService;
    }


    @Scheduled(fixedDelay = 60000) // Run every 60 seconds
    public void runCompaction() {
        System.out.println("Starting compaction...");
        logStore.compact(keyValueService.getAll());
        System.out.println("Compaction completed.");
    }

}
