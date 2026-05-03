# Async Replication Strategy

**File:** `AsyncReplicationStrategy.java`

---

## What It Does

Fire-and-forget replication strategy. Writes return to the client immediately, replication to peers happens in background threads. Reads return the local value immediately and trigger a background repair check.

### Write Path

1. Coordinator writes locally (if in the preference list for the key).
2. Fires `asyncSender.send(peer, ReplicationRequest)` for each peer in the preference list — non-blocking.
3. Returns `true` immediately without waiting for any peer acknowledgement.

### Read Path (`detectAndRepair`)

1. Returns `localValue` to the caller immediately.
2. Fires `CompletableFuture.runAsync(() -> detectAndRepair(...))` in the background.
3. `detectAndRepair` fetches the current value from all preference list peers.
4. Finds the entry with the highest Lamport timestamp across all responses.
5. Calls `repairStaleNodes` — sends the latest value to any node behind (including self via `putInternal`).

---

## Guarantees

- **Maximum availability**: writes never fail due to peer unavailability.
- **Eventual consistency**: all replicas converge to the same value given no new writes.
- **Self-healing reads**: every read triggers a background repair, so stale replicas converge over time without anti-entropy.

---

## Known Drawbacks

### 1. Reads can return stale or missing data
`read()` returns `localValue` immediately — even if it is null (key doesn't exist locally) or outdated. The background repair fixes this for future reads, but the current caller gets the wrong answer. A client reading immediately after a write routed to a different coordinator may see `null`.

### 2. No write acknowledgement from peers
The coordinator has no confirmation that peers received the write. If `AsyncSender` fails silently (e.g. peer is down but the hint also fails to store), the write is lost on that replica with no visibility.

### 3. `detectAndRepair` uses the value passed at read time
`localValue` is captured at the moment `read()` is called and passed into the async repair. If a newer write lands between the `store.get()` call and when `detectAndRepair` runs, the repair uses the old snapshot — potentially overwriting the new write on peers with an older value. This is a subtle race condition.

### 4. Background threads are unbounded
Every read fires a `CompletableFuture.runAsync()` using the default ForkJoinPool. Under high read load, this creates a large number of concurrent background tasks contending for HTTP connections and CPU.

### 5. No read guarantee
Unlike quorum reads, async reads give no guarantee about how stale the returned value can be. A node that has been partitioned for hours returns its last known value.

---

## Future Improvements

- **Configurable read modes**: expose a `consistent=true` query parameter that switches a single read to synchronous quorum behaviour without changing the global strategy.
- **Bounded async executor**: replace the default ForkJoinPool with a dedicated, bounded thread pool for repair tasks to prevent resource exhaustion under load.
- **Read repair result passthrough**: after `detectAndRepair` finds the latest value, return it to the original caller (rather than the stale local value), at the cost of blocking until repair completes. Makes async reads significantly stronger with a small latency trade-off.
- **Write confirmation callback**: have `AsyncSender` report back failed deliveries so the coordinator can decide whether to retry or surface an error.
