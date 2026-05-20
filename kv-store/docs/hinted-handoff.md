# Hinted Handoff

**Files:** `HintedHandoffService.java`, `HintStore.java`, `AsyncSender.java`

---

## What It Does

When a write to a peer fails (node down, network error), the coordinator stores a hint locally and retries delivery on a schedule. This allows writes to succeed (in async mode) or count toward quorum (in quorum mode) even when a target node is temporarily unavailable, and ensures the node catches up when it recovers.

### Write-Time Hint Storage

In `AsyncSender` (async mode) and `QuorumReplicationStrategy` (quorum mode):
```
peer unreachable during replicate()
    → hintStore.store(peer, new Hint(key, value, timestamp))
```

### Delivery (`HintedHandoffService`)

Delivery fires on two paths:

**Gossip-triggered (primary):** `GossipService.merge()` detects a DEAD→ALIVE recovery or a brand-new node joining the ring and immediately calls `hintedHandoffService.deliverHintsFor(node)` on a background thread. Hints reach the recovered node within one gossip round (~1s) rather than waiting for the next poll.

**Scheduled fallback:** runs every 5 seconds as a safety net for hints that gossip-triggered delivery missed (e.g. the node came up between gossip rounds or the gossip notification was lost). For each node with pending hints:
1. Drain all hints for that node from `HintStore`.
2. Attempt `POST /internal/replicate` for each hint.
3. If delivery succeeds — hint is discarded.
4. If delivery fails (node still down) — hints are restored and retried next cycle.

---

## Guarantees

- Writes to temporarily unavailable nodes are not lost as long as the coordinator stays alive.
- Delivery is retried indefinitely until the target node recovers.
- Delivered hints use `putInternal`, which applies last-write-wins — a hint will not overwrite a newer value the recovering node acquired through other means.

---

## Known Drawbacks

### 1. Delivery is sequential per node
Hints for a given node are delivered one by one in a loop. For a node recovering after a long outage with many hints, delivery serialises all of them through a single HTTP call chain and holds the delivery thread for the full duration.

### 2. Coordinator dependency
Hints are disk-persisted and survive coordinator restarts. However, hinted handoff only captures failures that happened during `replicate()` on this coordinator. A write that was coordinated by a different node stores its hints there — if that coordinator is replaced or permanently lost, those specific hints go with it. Anti-entropy is the safety net for that case.

### 4. Delivery is sequential per node
Hints for a given node are delivered one by one in a loop. For a node recovering after a long outage with thousands of hints, delivery is slow and ties up the scheduler thread.

### 5. Only covers write failures at replication time
Hinted handoff only captures failures that happen during `replicate()`. Silent divergence — for example, a node that was up and responded `true` but silently failed to persist — is not tracked. Anti-entropy handles that case.

---

## Future Improvements

- **Parallel delivery**: deliver all hints for a recovering node concurrently (bounded parallelism) rather than sequentially, for faster catch-up after long outages.
- **Batch delivery endpoint**: deliver all hints in a single `POST /internal/replicate/batch` call rather than one call per hint. Pairs well with parallel delivery.
