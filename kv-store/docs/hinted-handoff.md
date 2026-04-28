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

Runs every 5 seconds. For each node with pending hints:
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

### 1. Hints are in-memory only
`HintStore` is a `ConcurrentHashMap`. If the coordinator crashes before delivering hints, all buffered hints are lost. The recovering node must rely on anti-entropy to catch up instead.

### 2. No TTL on hints
Hints accumulate indefinitely if a node stays down. On a busy cluster, a prolonged outage on one node can cause the coordinator's hint buffer to grow without bound, consuming memory.

### 3. Coordinator dependency
Hinted handoff only works if the coordinator that stored the hint is the one that delivers it. If the coordinator is replaced or restarted (without persisting hints), the target node misses those writes entirely and must wait for anti-entropy.

### 4. Delivery is sequential per node
Hints for a given node are delivered one by one in a loop. For a node recovering after a long outage with thousands of hints, delivery is slow and ties up the scheduler thread.

### 5. Only covers write failures at replication time
Hinted handoff only captures failures that happen during `replicate()`. Silent divergence — for example, a node that was up and responded `true` but silently failed to persist — is not tracked. Anti-entropy handles that case.

---

## Future Improvements

- **Persist hints to disk**: write hints to a WAL alongside the regular key-value log so they survive coordinator restarts. This eliminates the coordinator-dependency problem.
- **TTL and size cap**: evict hints older than a configurable threshold (e.g. 1 hour) and cap the buffer size. Beyond that threshold, anti-entropy is expected to cover the gap.
- **Parallel delivery**: deliver all hints for a recovering node concurrently (bounded parallelism) rather than sequentially, for faster catch-up.
- **Gossip-triggered delivery**: instead of polling every 5 seconds, trigger hint delivery immediately when the gossip layer detects the target node has come back online.
- **Batch delivery endpoint**: deliver all hints in a single `POST /internal/replicate/batch` call rather than one call per hint.
