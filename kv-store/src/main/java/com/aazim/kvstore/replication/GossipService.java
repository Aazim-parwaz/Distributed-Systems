package com.aazim.kvstore.replication;

import com.aazim.kvstore.model.MemberInfo;
import com.aazim.kvstore.model.NodeState;

import jakarta.annotation.PostConstruct;
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

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class GossipService {

    private static final Logger log = LoggerFactory.getLogger(GossipService.class);
    private static final int FANOUT = 3;
    private static final int INDIRECT_PROBE_PEERS = 2;

    private final ConcurrentHashMap<String, MemberInfo> members = new ConcurrentHashMap<>();
    private final RestTemplate restTemplate;
    private final ConsistentHashRing ring;
    private final WebServerApplicationContext context;
    private final Random random = new Random();

    @Value("${Nodes}")
    private String nodesConfig;

    @Value("${node.id}")
    private String nodeId;

    @Value("${gossip.suspect.timeout.ms:5000}")
    private long suspectTimeout;

    @Value("${gossip.dead.timeout.ms:10000}")
    private long deadTimeout;

    private String selfNode;
    private long ownIncarnation;

    public GossipService(RestTemplate restTemplate, ConsistentHashRing ring,
                         WebServerApplicationContext context) {
        this.restTemplate = restTemplate;
        this.ring = ring;
        this.context = context;
    }

    // Runs at startup before the web server is ready. Loads the persisted incarnation
    // number and increments it so this run is distinguishable from all prior runs.
    // A restarted node's heartbeat resets to 0, but its incarnation is always higher
    // than what the cluster has cached — so gossip will accept its updates again.
    @PostConstruct
    public void loadIncarnation() {
        File file = new File("incarnation_" + nodeId + ".dat");
        long current = 0;
        if (file.exists()) {
            try (BufferedReader r = new BufferedReader(new FileReader(file))) {
                String line = r.readLine();
                if (line != null) current = Long.parseLong(line.trim());
            } catch (Exception e) {
                log.warn("Could not read incarnation file, starting from 0");
            }
        }
        ownIncarnation = current + 1;
        try (PrintWriter w = new PrintWriter(new FileWriter(file))) {
            w.println(ownIncarnation);
        } catch (Exception e) {
            log.error("Failed to persist incarnation: {}", e.getMessage());
        }
        log.info("Node incarnation: {}", ownIncarnation);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        selfNode = "localhost:" + context.getWebServer().getPort();
        long now = monotonicMs();

        members.put(selfNode, new MemberInfo(selfNode, NodeState.ALIVE, 0, now, ownIncarnation));

        Arrays.stream(nodesConfig.split(","))
              .map(String::trim)
              .filter(n -> !n.isEmpty() && !n.equals(selfNode))
              .forEach(n -> members.put(n, new MemberInfo(n, NodeState.ALIVE, 0, now, 0)));

        log.info("Gossip initialized on {}, seeded {} peers", selfNode, members.size() - 1);
    }

    @Scheduled(fixedDelayString = "${gossip.interval.ms:1000}")
    public void gossipRound() {
        if (selfNode == null) return;

        members.compute(selfNode, (k, v) ->
                new MemberInfo(k, NodeState.ALIVE,
                        v == null ? 1 : v.getHeartbeat() + 1,
                        monotonicMs(),
                        ownIncarnation));

        detectFailures();

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

    public Map<String, MemberInfo> merge(Map<String, MemberInfo> incoming) {
        long now = monotonicMs();

        for (Map.Entry<String, MemberInfo> entry : incoming.entrySet()) {
            String node = entry.getKey();
            MemberInfo received = entry.getValue();

            boolean isNew = !members.containsKey(node);

            members.merge(node, received, (existing, recv) -> {
                // Stale info from a previous run of this node — ignore.
                if (recv.getIncarnation() < existing.getIncarnation()) return existing;

                // Same incarnation and no newer heartbeat — nothing to update.
                if (recv.getIncarnation() == existing.getIncarnation()
                        && recv.getHeartbeat() <= existing.getHeartbeat()) return existing;

                NodeState newState = recv.getState() == NodeState.DEAD ? NodeState.DEAD : NodeState.ALIVE;

                if (existing.getState() == NodeState.DEAD && newState == NodeState.ALIVE) {
                    ring.addNode(node);
                    log.info("Node {} recovered (incarnation {}), re-added to ring", node, recv.getIncarnation());
                }
                return new MemberInfo(node, newState, recv.getHeartbeat(), now, recv.getIncarnation());
            });

            // Only add genuinely new, non-dead nodes to the ring.
            if (isNew && received.getState() != NodeState.DEAD) {
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
                // Before declaring dead, ask random live peers to try reaching the node.
                // A GC-paused or high-load node may be unreachable from us but fine from others.
                if (indirectProbeSucceeds(node)) {
                    members.put(node, new MemberInfo(node, NodeState.ALIVE, info.getHeartbeat(), monotonicMs(), info.getIncarnation()));
                    log.info("Node {} passed indirect probe, kept alive", node);
                } else {
                    members.put(node, new MemberInfo(node, NodeState.DEAD, info.getHeartbeat(), info.getLastSeen(), info.getIncarnation()));
                    ring.removeNode(node);
                    log.warn("Node {} declared dead ({}ms elapsed, indirect probes failed), removed from ring", node, elapsed);
                }
            } else if (elapsed > suspectTimeout && info.getState() == NodeState.ALIVE) {
                members.put(node, new MemberInfo(node, NodeState.SUSPECT, info.getHeartbeat(), info.getLastSeen(), info.getIncarnation()));
                log.warn("Node {} is suspect ({}ms since last heartbeat)", node, elapsed);
            }
        });
    }

    // Asks up to INDIRECT_PROBE_PEERS random live nodes to try reaching the target.
    // Returns true if any peer confirms the target is reachable.
    private boolean indirectProbeSucceeds(String target) {
        List<String> probers = members.entrySet().stream()
                .filter(e -> !e.getKey().equals(selfNode)
                        && !e.getKey().equals(target)
                        && e.getValue().getState() == NodeState.ALIVE)
                .map(Map.Entry::getKey)
                .limit(INDIRECT_PROBE_PEERS)
                .toList();

        for (String prober : probers) {
            try {
                Boolean reachable = restTemplate.postForObject(
                        "http://" + prober + "/kv/internal/probe?target=" + target,
                        null, Boolean.class);
                if (Boolean.TRUE.equals(reachable)) return true;
            } catch (Exception e) {
                log.debug("Indirect probe via {} to {} failed: {}", prober, target, e.getMessage());
            }
        }
        return false;
    }

    // Called by the probe endpoint — tries to reach target and reports reachability.
    public boolean canReach(String target) {
        try {
            restTemplate.getForObject("http://" + target + "/kv/members", Object.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public List<String> getLiveMembers() {
        return members.entrySet().stream()
                .filter(e -> e.getValue().getState() != NodeState.DEAD)
                .map(Map.Entry::getKey)
                .toList();
    }

    public Map<String, MemberInfo> getMembers() {
        return Collections.unmodifiableMap(members);
    }

    private static long monotonicMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    }
}
