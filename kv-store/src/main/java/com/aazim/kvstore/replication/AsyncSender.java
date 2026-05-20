package com.aazim.kvstore.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.aazim.kvstore.model.Hint;
import com.aazim.kvstore.model.ReplicationRequest;

@Component
public class AsyncSender {

    private static final Logger log = LoggerFactory.getLogger(AsyncSender.class);

    private final RestTemplate restTemplate;
    private final HintStore hintStore;

    public AsyncSender(RestTemplate restTemplate, HintStore hintStore) {
        this.restTemplate = restTemplate;
        this.hintStore = hintStore;
    }

    @Async
    public void send(String node, ReplicationRequest request) {
        try {
            restTemplate.postForObject("http://" + node + "/kv/internal/replicate", request, Boolean.class);
            log.info("Replicated to {} key={}", node, request.getKey());
        } catch (Exception e) {
            log.warn("Failed to reach {}, storing hint for key={}", node, request.getKey());
            hintStore.store(node, new Hint(request.getKey(), request.getValue(), request.getTimestamp(), request.isDeleted()));
        }
    }
}
