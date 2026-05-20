package com.aazim.kvstore.replication;

import com.aazim.kvstore.model.ReplicationRequest;
import com.aazim.kvstore.model.ValueEntry;
import com.aazim.kvstore.service.KeyValService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Component
public class AntiEntropyService {

    private static final Logger log = LoggerFactory.getLogger(AntiEntropyService.class);

    private final RestTemplate restTemplate;
    private final KeyValService keyValService;
    private final ConsistentHashRing ring;
    private final GossipService gossipService;
    private final WebServerApplicationContext context;

    @Value("${replication.factor:3}")
    private int replicationFactor;

    private String selfNode;

    public AntiEntropyService(RestTemplate restTemplate, @Lazy KeyValService keyValService,
                              ConsistentHashRing ring, GossipService gossipService,
                              WebServerApplicationContext context) {
        this.restTemplate = restTemplate;
        this.keyValService = keyValService;
        this.ring = ring;
        this.gossipService = gossipService;
        this.context = context;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        selfNode = "localhost:" + context.getWebServer().getPort();
    }

    @Scheduled(fixedDelayString = "${anti.entropy.delay.ms:30000}")
    public void runAntiEntropy() {
        if (selfNode == null) return;

        List<String> peers = gossipService.getLiveMembers().stream()
                .filter(n -> !n.equals(selfNode))
                .toList();

        for (String peer : peers) {
            try {
                syncWithPeer(peer);
            } catch (Exception e) {
                log.warn("Anti-entropy sync failed for {}: {}", peer, e.getMessage());
            }
        }
    }

    private void syncWithPeer(String peer) {
        MerkleTree localTree = new MerkleTree(keyValService.getAll());

        String[] peerHashes;
        try {
            peerHashes = restTemplate.getForObject(
                    "http://" + peer + "/kv/internal/merkle/tree", String[].class);
        } catch (Exception e) {
            log.warn("Failed to fetch merkle tree from {}: {}", peer, e.getMessage());
            return;
        }

        if (peerHashes == null) return;

        // Diff both trees locally — no further HTTP calls until a diverged bucket is found.
        findDivergedBuckets(peer, localTree, peerHashes, 0);
    }

    private void findDivergedBuckets(String peer, MerkleTree localTree, String[] peerHashes, int nodeIndex) {
        if (nodeIndex >= peerHashes.length) return;
        if (localTree.getHash(nodeIndex).equals(peerHashes[nodeIndex])) return;

        if (localTree.isLeaf(nodeIndex)) {
            syncBucket(peer, localTree, localTree.bucketIndexForLeaf(nodeIndex));
        } else {
            findDivergedBuckets(peer, localTree, peerHashes, localTree.leftChild(nodeIndex));
            findDivergedBuckets(peer, localTree, peerHashes, localTree.rightChild(nodeIndex));
        }
    }

    private void syncBucket(String peer, MerkleTree localTree, int bucketIndex) {
        Map<String, ValueEntry> peerEntries;
        try {
            peerEntries = restTemplate.exchange(
                    "http://" + peer + "/kv/internal/merkle/bucket/" + bucketIndex,
                    HttpMethod.GET, null,
                    new ParameterizedTypeReference<Map<String, ValueEntry>>() {}
            ).getBody();
        } catch (Exception e) {
            log.warn("Failed to fetch bucket {} from {}: {}", bucketIndex, peer, e.getMessage());
            return;
        }

        if (peerEntries == null) return;

        Map<String, ValueEntry> localEntries = localTree.getBucketEntries(bucketIndex);

        for (Map.Entry<String, ValueEntry> e : peerEntries.entrySet()) {
            // Only pull keys this node is supposed to own.
            if (!ring.getPreferenceList(e.getKey(), replicationFactor).contains(selfNode)) continue;
            ValueEntry local = localEntries.get(e.getKey());
            if (local == null || e.getValue().getTimestamp() > local.getTimestamp()) {
                keyValService.putInternal(e.getKey(), e.getValue().getValue(), e.getValue().getTimestamp(), e.getValue().isDeleted());
                log.debug("Anti-entropy: pulled key={} deleted={} from {}", e.getKey(), e.getValue().isDeleted(), peer);
            }
        }

        for (Map.Entry<String, ValueEntry> e : localEntries.entrySet()) {
            // Only push keys the peer is supposed to own.
            if (!ring.getPreferenceList(e.getKey(), replicationFactor).contains(peer)) continue;
            ValueEntry peerVal = peerEntries.get(e.getKey());
            if (peerVal == null || e.getValue().getTimestamp() > peerVal.getTimestamp()) {
                try {
                    restTemplate.postForObject(
                            "http://" + peer + "/kv/internal/replicate",
                            new ReplicationRequest(e.getKey(), e.getValue().getValue(), e.getValue().getTimestamp(), e.getValue().isDeleted()),
                            Boolean.class);
                    log.debug("Anti-entropy: pushed key={} deleted={} to {}", e.getKey(), e.getValue().isDeleted(), peer);
                } catch (Exception ex) {
                    log.warn("Anti-entropy: failed to push key={} to {}: {}", e.getKey(), peer, ex.getMessage());
                }
            }
        }
    }
}
