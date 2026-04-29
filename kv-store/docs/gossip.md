# Gossip Protocol

**Files:** `GossipService.java`, `model/MemberInfo.java`, `model/NodeState.java`

---

## What It Does

Replaces static node configuration with a decentralized membership protocol. Every node maintains a local view of the cluster — who is alive, who is suspect, who is dead — and propagates that view by exchanging membership tables with random peers on a fixed interval.

### Membership Table

Each node maintains a `ConcurrentHashMap<String, MemberInfo>` keyed by node address. Each entry holds:

- `address` — the node's host:port
- `state` — `ALIVE`, `SUSPECT`, or `DEAD`
- `heartbeat` — a monotonically incrementing counter owned by the node itself
- `lastSeen` — local monotonic time (ms via `System.nanoTime()`) when this node last observed a heartbeat advance for that peer. Never propagated in gossip messages.

### Gossip Round (every `gossip.interval.ms`, default 1s)

1. Increment own heartbeat counter.
2. Run failure detection against current membership table.
3. Pick up to 3 random non-dead peers (`FANOUT = 3`).
4. For each selected peer: `POST /internal/gossip` with the full local membership table, receive the peer's table in response, merge it.

### Merge Rule

For each node entry in an incoming table:

- If incoming heartbeat > known heartbeat → heartbeat advanced, node is reachable. Update state to `ALIVE`, reset `lastSeen` to local monotonic time.
- If incoming heartbeat <= known heartbeat → stale or duplicate, discard.

`lastSeen` is always set from the receiver's own clock, never from the value in the incoming message. This prevents stale timestamps from propagating through the cluster.

### Failure Detection

Runs at the start of every gossip round. For each peer (excluding self):

```
elapsed = monotonicMs() - lastSeen

elapsed > suspectTimeout (default 5s)  AND state == ALIVE   →  ALIVE → SUSPECT
elapsed > deadTimeout   (default 10s)  AND state != DEAD    →  → DEAD → ring.removeNode()
```

When a node transitions to `DEAD`, it is removed from the consistent hash ring immediately. The replication preference list shrinks, and traffic is no longer routed to that node.

### Recovery

When a `DEAD` node comes back and resumes sending heartbeats, a gossip round eventually receives a higher heartbeat counter for it. The merge logic detects the `DEAD → ALIVE` transition and calls `ring.addNode()`. Anti-entropy then syncs any data the node missed while it was down.

---

## Clock Usage

Two distinct clocks are used and they must not be confused:

| Field | Clock | Reason |
|---|---|---|
| `heartbeat` | Logical counter (`+1` per round) | Orders membership updates across nodes. Immune to wall-clock drift. Comparable across nodes. |
| `lastSeen` | `System.nanoTime()` (monotonic) | Measures local elapsed time for timeout detection. Never crosses node boundaries. Immune to NTP adjustments. |

`System.currentTimeMillis()` is not used anywhere in the gossip path. Wall time cannot be trusted for elapsed-time measurements because NTP adjustments can move it backward (preventing failure detection) or forward (triggering false positives).

---

## Guarantees

- Membership changes propagate in O(log N) gossip rounds on average (infection-style spread with fanout 3).
- A dead node is removed from the ring within `deadTimeout` ms of its last successful gossip.
- A recovered node is re-added to the ring within one gossip round after its heartbeat advances.
- `lastSeen` is always local — no node trusts another node's timestamp for failure detection decisions.
- The ring is updated atomically relative to gossip (all ring methods are `synchronized`).

---

## Known Drawbacks

### 1. Seeds are still static config
The initial peer list comes from `${Nodes}`. A completely new node joining a running cluster must be listed in config and requires a restart. There is no zero-config discovery.

### 2. DEAD nodes are never fully forgotten
Once a node enters `DEAD` state, it stays in the membership table indefinitely. In a long-running cluster with frequent node replacements, the table grows without bound.

### 3. No distinction between crash and network partition
A node on the other side of a partition looks identical to a crashed node — both stop sending heartbeats. Gossip will declare it dead and remove it from the ring, potentially causing the ring to shrink below the replication factor on both sides of the partition. This is the standard availability vs. consistency trade-off in a leaderless system.

### 4. False positives under load
The suspect and dead timeouts are fixed. If a node is briefly overloaded (GC pause, CPU spike) and misses a few gossip rounds, it may be incorrectly marked suspect or even dead before it recovers. A phi-accrual failure detector adapts the threshold dynamically based on observed inter-arrival times, reducing false positives under variable load.

### 5. Gossip table grows with cluster size
Every gossip round sends the full membership table. In a large cluster (hundreds of nodes), this becomes a significant payload. The standard fix is to gossip only a delta (entries changed since last round) rather than the full table.

### 6. No gossip encryption or authentication
Any process that can reach `/internal/gossip` can inject arbitrary membership state. In a production system, internal endpoints would be restricted to a private network or authenticated with mutual TLS.

---

## Future Improvements

- **Phi accrual failure detector**: replace fixed timeouts with a dynamic threshold derived from the statistical distribution of inter-arrival heartbeat times. Reduces false positives on nodes under variable load.
- **Delta gossip**: send only entries whose heartbeat has changed since the last round rather than the full table. Reduces per-round payload from O(N) to O(changed entries).
- **Tombstone expiry**: after a node has been `DEAD` for a configurable TTL, remove it from the table entirely to bound memory usage.
- **Zero-config seed discovery**: use multicast or a DNS SRV record for initial peer discovery so new nodes can join without config changes.
- **SWIM-style piggybacked failure notifications**: when gossip detects a node is unreachable, immediately notify other peers rather than waiting for them to discover it independently, accelerating convergence after failures.
