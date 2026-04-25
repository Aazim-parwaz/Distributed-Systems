package com.aazim.kvstore.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.aazim.kvstore.model.Hint;
import com.aazim.kvstore.model.ReplicationRequest;

@Component
public class HintedHandoffService {

    private static final Logger log = LoggerFactory.getLogger(HintedHandoffService.class);

    private final HintStore hintStore;
    private final RestTemplate restTemplate;

    public HintedHandoffService(HintStore hintStore, RestTemplate restTemplate) {
        this.hintStore = hintStore;
        this.restTemplate = restTemplate;
    }

    @Scheduled(fixedDelay = 5000)
    public void deliverPendingHints() {
        Set<String> nodes = hintStore.nodesWithHints();
        if (!nodes.isEmpty()) {
            log.info("Hint delivery tick — pending nodes: {}", nodes);
        }

        for (String node : nodes) {
            List<Hint> hints = hintStore.drain(node);
            if (hints.isEmpty()) continue;

            List<Hint> undelivered = new ArrayList<>();
            boolean nodeDown = false;

            for (Hint hint : hints) {
                if (nodeDown) {
                    undelivered.add(hint);
                    continue;
                }
                try {
                    restTemplate.postForObject(
                        "http://" + node + "/kv/internal/replicate",
                        new ReplicationRequest(hint.getKey(), hint.getValue(), hint.getTimestamp()),
                        Boolean.class);
                    log.info("Delivered hint to {} key={}", node, hint.getKey());
                } catch (Exception e) {
                    log.warn("{} still unreachable, requeueing {} hints", node, hints.size() - undelivered.size());
                    undelivered.add(hint);
                    nodeDown = true;
                }
            }

            if (!undelivered.isEmpty()) {
                hintStore.restore(node, undelivered);
            } else {
                log.info("All {} hints delivered to {}", hints.size(), node);
            }
        }
    }
}
