package com.aazim.kvstore.replication;

import com.aazim.kvstore.model.MemberInfo;
import com.aazim.kvstore.model.NodeState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.event.EventListener;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class GossipService {

    private static final Logger log = LoggerFactory.getLogger(GossipService.class);
    private static final int FANOUT = 3;

    private final ConcurrentHashMap<String, MemberInfo> members = new ConcurrentHashMap<>();
    private final RestTemplate restTemplate;
    private final ConsistentHashRing ring;
    private final WebServerApplicationContext context;
    private final Random random = new Random();

    @Value("${Nodes}")
    private String nodesConfig;

    @Value("${gossip.suspect.timeout.ms:5000}")
    private long suspectTimeout;

    @Value("${gossip.dead.timeout.ms:10000}")
    private long deadTimeout;

    private String selfNode;

    public GossipService(RestTemplate restTemplate, ConsistentHashRing ring,
                         WebServerApplicationContext context) {
        this.restTemplate = restTemplate;
        this.ring = ring;
        this.context = context;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        selfNode = "localhost:" + context.getWebServer().getPort();
        long now = monotonicMs();

        members.put(selfNode, new MemberInfo(selfNode, NodeState.ALIVE, 0, now));

        // Seed the table with all known nodes. lastSeen = now gives them
        // suspectTimeout grace before failure detection kicks in.
        Arrays.stream(nodesConfig.split(","))
              .map(String::trim)
              .filter(n -> !n.isEmpty() && !n.equals(selfNode))
              .forEach(n -> members.put(n, new MemberInfo(n, NodeState.ALIVE, 0, now)));

        log.info("Gossip initialized on {}, seeded {} peers", selfNode, members.size() - 1);
    }

    @Scheduled(fixedDelayString = "${gossip.interval.ms:1000}")
    public void gossipRound() {
        if (selfNode == null) return;

        // Advance own heartbeat.
        members.compute(selfNode, (k, v) ->
                new MemberInfo(k, NodeState.ALIVE,
                        v == null ? 1 : v.getHeartbeat() + 1,
                        monotonicMs()));

        detectFailures();

        // Pick up to FANOUT non-dead peers at random.
        List<String> candidates = members.entrySet().stream()
                .filter(e -> !e.getKey().equals(selfNode) && e.getValue().getState() != NodeState.DEAD)
                .map(Map.Entry::getKey)
                .collect(Collectors.toCollection(ArrayList::new));

        Collections.shuffle(candidates, random);
        candidates.stream().limit(FANOUT).forEach(this::gossipWith);
    }

    private void gossipWith(String peer) {
        try {
            Map<String, MemberInfo> peerTable = restTemplate.exchange(
                    "http://" + peer + "/kv/internal/gossip",
                    HttpMethod.POST,
                    new HttpEntity<>(new HashMap<>(members)),
                    new ParameterizedTypeReference<Map<String, MemberInfo>>() {}
            ).getBody();

            if (peerTable != null) merge(peerTable);
        } catch (Exception e) {
            log.debug("Gossip to {} failed: {}", peer, e.getMessage());
        }
    }

    // Called both when we receive a gossip push and when we reply to one.
    // Returns our current membership table so the caller can merge it too.
    public Map<String, MemberInfo> merge(Map<String, MemberInfo> incoming) {
        long now = monotonicMs();

        for (Map.Entry<String, MemberInfo> entry : incoming.entrySet()) {
            String node = entry.getKey();
            MemberInfo received = entry.getValue();

            boolean isNew = !members.containsKey(node);

            members.merge(node, received, (existing, recv) -> {
                if (recv.getHeartbeat() <= existing.getHeartbeat()) return existing;

                NodeState newState = recv.getState() == NodeState.DEAD ? NodeState.DEAD : NodeState.ALIVE;

                if (existing.getState() == NodeState.DEAD && newState == NodeState.ALIVE) {
                    ring.addNode(node);
                    log.info("Node {} recovered, re-added to ring", node);
                }
                return new MemberInfo(node, newState, recv.getHeartbeat(), now);
            });

            if (isNew) {
                ring.addNode(node);
                log.info("Discovered new node {}, added to ring", node);
            }
        }

        return Collections.unmodifiableMap(members);
    }

    private void detectFailures() {
        long now = monotonicMs();
        members.forEach((node, info) -> {
            if (node.equals(selfNode) || info.getState() == NodeState.DEAD) return;

            long elapsed = now - info.getLastSeen();

            if (elapsed > deadTimeout) {
                members.put(node, new MemberInfo(node, NodeState.DEAD, info.getHeartbeat(), info.getLastSeen()));
                ring.removeNode(node);
                log.warn("Node {} declared dead ({}ms since last heartbeat), removed from ring", node, elapsed);
            } else if (elapsed > suspectTimeout && info.getState() == NodeState.ALIVE) {
                members.put(node, new MemberInfo(node, NodeState.SUSPECT, info.getHeartbeat(), info.getLastSeen()));
                log.warn("Node {} is suspect ({}ms since last heartbeat)", node, elapsed);
            }
        });
    }

    // Returns all nodes that are ALIVE or SUSPECT (still potentially reachable).
    public List<String> getLiveMembers() {
        return members.entrySet().stream()
                .filter(e -> e.getValue().getState() != NodeState.DEAD)
                .map(Map.Entry::getKey)
                .toList();
    }

    public Map<String, MemberInfo> getMembers() {
        return Collections.unmodifiableMap(members);
    }

    // Monotonic milliseconds — immune to NTP adjustments and system clock changes.
    // Used exclusively for lastSeen, which is a local elapsed-time measurement.
    // Never compare these values across nodes; they have no absolute meaning.
    private static long monotonicMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }
}
