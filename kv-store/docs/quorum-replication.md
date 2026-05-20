# Quorum Replication Strategy

**File:** `QuorumReplicationStrategy.java`

---

## What It Does

Synchronous replication strategy. Writes block until a quorum of nodes acknowledge, and reads fan out to all preference list nodes in parallel to guarantee the latest value is returned.

### Write Path

1. Compute the preference list for the key via the consistent hash ring.
2. Count self as ACK #1 if this node is in the preference list.
3. Send `POST /internal/replicate` to each peer sequentially.
4. Return `true` as soon as `successCount >= effectiveQuorum`. Stop early.
5. If a peer is unreachable, store a hint and continue to the next peer.
6. If quorum is never met, return `false`.

Effective quorum: `max(configured_quorum, floor(N/2) + 1)` — always at least majority.

### Read Path

1. Fan out `GET /internal/get` to all preference list peers in parallel.
2. Wait for all to respond (or fail).
3. If fewer than `R = floor(N/2) + 1` nodes responded, throw — read quorum not met.
4. Pick the entry with the highest Lamport timestamp.
5. Asynchronously repair any stale or missing replicas (`repairNodes`).
6. Return the latest value to the client.

---

## Guarantees

- **Read-your-writes**: W + R > N ensures at least one node that acknowledged the write will be included in every read quorum.
- **Latest value on read**: The highest Lamport timestamp across all responding nodes is always returned.
- **Read repair**: Stale replicas are healed after every quorum read (asynchronously).
- **Hinted handoff**: Writes to unreachable peers are buffered and replayed on recovery.

---

## Known Drawbacks

### 1. Partial writes on quorum failure are not rolled back
If some peers succeed before quorum fails, those writes persist on those nodes. The coordinator does not write locally (since `success = false`), and no rollback is sent to peers that did succeed. These "orphaned" writes surface on the next read — a write the client considers failed can be returned by a future read and propagated by read repair.

### 2. Write quorum is checked on ACK count, not durability
Self is counted as ACK #1 before the local write actually happens. If quorum is met (self + one peer), `replicate()` returns `true` and the local write happens in `KeyValService`. This is fine when quorum is met, but the pre-count means a failed round still counted self even though nothing was written locally.

### 3. Read quorum checks responder count, not valid-entry count
```java
if (responses.size() < readQuorum) throw ...  // counts responders
```
A node that responds with `null` (key not found) counts toward the quorum. A single replica holding a value is enough for it to win and be propagated via read repair, even if the original write was considered failed.

### 4. All peers always contacted on writes (minor latency overhead)
All preference list peers are contacted in parallel and the write waits for the slowest to respond (up to the 1s `orTimeout` ceiling). Quorum is checked after all futures settle. A future optimisation is to short-circuit as soon as W ACKs arrive, cancelling remaining futures — at the cost of leaving some replicas unwritten until anti-entropy catches up.

### 5. Full fan-out on reads
Every read contacts all preference list nodes in parallel. With replication factor 3, every read causes 2 extra internal HTTP calls even when the local value is up to date.

---

## Future Improvements

- **Short-circuit on quorum**: Currently all peer futures run to completion (or timeout). Return as soon as W ACKs arrive and cancel remaining futures to reduce tail latency when quorum is met early.
- **Sloppy quorum**: Allow writes to overflow to non-preference-list nodes when preference list nodes are down, with hinted handoff back to the real owner. Improves availability under partial failures.
- **Versioned responses on reads**: Return a version/token alongside the value so clients can do conditional writes (compare-and-swap), enabling stronger consistency guarantees.
- **Timeout per peer**: Configurable per-peer timeout on the write path so a slow peer does not stall the entire write.
