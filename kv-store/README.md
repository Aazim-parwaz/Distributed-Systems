# Distributed Key-Value Store

A Dynamo-style distributed key-value store built from scratch in Java (Spring Boot). Implements consistent hashing, quorum-based replication, read repair, hinted handoff, anti-entropy via Merkle trees, and a Lamport clock for conflict resolution — all without any external coordination service.

---

## Features

| Feature | Description |
|---|---|
| Consistent hashing | Keys are partitioned across nodes using a virtual-node ring (150 vnodes/node) |
| Two replication modes | `async` (high availability) or `quorum` (stronger consistency) — switchable via config |
| Lamport clock LWW | Monotonically increasing logical timestamps; last-write-wins conflict resolution |
| Read repair | Stale replicas are healed automatically on every read |
| Hinted handoff | Writes to down nodes are buffered and replayed when the node recovers |
| Anti-entropy | Merkle tree comparison between peers proactively finds and repairs all divergence |
| WAL persistence | Every write is appended to a per-node log file; full state recovers on restart |
| Log compaction | WAL is compacted every 60 seconds, keeping only the latest value per key |

---

## Architecture

```
+-------------------------------------------------------------+
|                        Client                               |
+------------------+------------------+-----------------------+
                   | PUT/GET          |
                   v                  v
+--------------------------------------------------------------+
|               Any node (coordinator)                         |
|                                                              |
|  +------------------+    +------------------------------+   |
|  |  KeyValService   |    |     ConsistentHashRing       |   |
|  |  - put()         +---->  - getPreferenceList(key, N) |   |
|  |  - read()        |    |  - 150 vnodes / physical node|   |
|  |  - putInternal() |    +------------------------------+   |
|  +--------+---------+                                        |
|           |                                                  |
|           v                                                  |
|  +---------------------------------------------------+      |
|  |              ReplicationStrategy                   |      |
|  |   Quorum: waits for W ACKs before returning       |      |
|  |   Async:  fire-and-forget, returns immediately    |      |
|  +------------+---------------------------+----------+      |
|               | success                   | failure         |
|               v                           v                 |
|  +--------------------+   +----------------------------+   |
|  |  Replica nodes     |   |       HintStore             |   |
|  |  /internal/        |   |  Buffers write for dead node|   |
|  |  replicate         |   +----------------------------+   |
|  +--------------------+               |                     |
|                              +--------v-----------------+   |
|                              |  HintedHandoffService    |   |
|                              |  Retries every 5 seconds |   |
|                              +--------------------------+   |
|                                                             |
|  +--------------------------------------------------------+ |
|  |              AntiEntropyService (background)           | |
|  |  - Runs every N seconds (configurable)                 | |
|  |  - Builds Merkle tree over local store                 | |
|  |  - Compares root hash with each peer                   | |
|  |  - Walks tree top-down to find diverged buckets        | |
|  |  - Bidirectional sync respecting ring ownership        | |
|  +--------------------------------------------------------+ |
+--------------------------------------------------------------+
```

### Storage layer (per node)

```
InMemoryStore (HashMap)
      |
      |  written on every committed write
      v
FileLogStore  (kvstore_{port}.log)
      |
      |  compacted every 60s
      v
  key,value,timestamp  <-- one line per write
```

---

## Consistent Hashing

Each physical node is assigned 150 virtual nodes (vnodes) on a ring by hashing `node#0` through `node#149` with MD5. For any key, the preference list is the N distinct physical nodes found by walking clockwise from the key's hash position.

```
        hash("foo")
             |
             v
  -- 8080 -- 8080 -- 8082 -- 8081 -- 8082 -- 8081 -->  ring
                       ^              |
                       +--------------+
                  preference list for "foo" = [8082, 8081, 8080]  (RF=3)
```

A coordinator only stores data locally if it appears in the preference list. Writes are replicated exclusively to preference list nodes, not to the entire cluster. With 4 nodes and replication factor 3, each key is owned by exactly 3 of the 4 nodes — the 4th node never stores it.

---

## Replication Modes

### Async (default)

- Writes return immediately to the client.
- Replication to peer owners happens in background threads (`@Async`).
- Failed replications store a hint; delivery is retried every 5 seconds.
- Reads return the local value immediately, then compare with peers in the background and repair any stale nodes.
- **Guarantee:** eventual consistency, maximum availability.

### Quorum

Configured via `replication.mode=quorum`.

- Write quorum: `W = max(configured_quorum, floor(N/2) + 1)` where N = preference list size.
- Read quorum: `R = floor(N/2) + 1`.
- W + R > N ensures at least one node has seen the latest write on every read.
- Reads fan out to all preference list nodes in parallel, pick the highest Lamport timestamp, and asynchronously repair any stale replicas.
- **Guarantee:** strongly consistent reads under non-partitioned conditions.

---

## Anti-Entropy

Anti-entropy runs as a background scheduler on every node and proactively repairs diverged replicas — without waiting for a read to trigger repair.

### How it works

1. Build a **Merkle tree** over the local store. Keys are distributed into 16 buckets by `hashCode() % 16`. Each bucket hashes its sorted key-value-timestamp pairs with SHA-256. Internal nodes hash their children bottom-up.
2. **Compare root hash** with each peer. If roots match, the entire keyspace is in sync — no further work.
3. **Walk the tree top-down** when roots differ. Skip matching subtrees entirely. Only diverged leaf buckets trigger data exchange.
4. For each diverged bucket, **bidirectional sync**: pull newer entries from the peer, push newer local entries to the peer.

### Ring-aware sync

Anti-entropy respects consistent hashing — it does not blindly replicate keys to every peer:

- **Pull**: only accepts a key from a peer if this node is in the preference list for that key.
- **Push**: only sends a key to a peer if that peer is in the preference list for that key.

This ensures that adding a 4th (or Nth) node does not cause keys to leak outside their designated owners.

### Hinted handoff vs anti-entropy

Both mechanisms repair diverged nodes, but they are complementary — neither replaces the other:

| Scenario | Hinted Handoff | Anti-entropy |
|---|---|---|
| Node down briefly, coordinator survives | Fast delivery on recovery | Also catches it (slower) |
| Coordinator crashes before delivering hints | Hints lost | Catches it |
| Quorum write partially succeeded | Not tracked | Catches it |
| Node rejoins after long partition | Hints may be lost | Full state sync |
| Silent divergence from any bug | No hint created | Catches it |

---

## Request Flow

### Write

```
PUT /kv/put?key=foo&value=bar
        |
        +-- 1. clock.tick()  -->  assign Lamport timestamp
        +-- 2. ring.getPreferenceList("foo", 3)  -->  [8081, 8082, 8080]
        |
        +-- [Quorum]
        |    +-- Replicate to 8081, 8082 (synchronous, with timeout)
        |    +-- If peer unreachable --> store hint, try next peer
        |    +-- Quorum met? --> write locally (if coordinator is in list)
        |    +-- Return SUCCESS / FAILURE
        |
        +-- [Async]
             +-- asyncSender.send(8081), asyncSender.send(8082)  [background]
             +-- Write locally (if coordinator is in preference list)
             +-- Return SUCCESS immediately (before peers have ACK'd)
```

### Read

```
GET /kv/get?key=foo
        |
        +-- [Quorum]
        |    +-- Fan out GET /internal/get to all preference list nodes (parallel)
        |    +-- Wait for >= R nodes to respond
        |    +-- Pick entry with highest Lamport timestamp
        |    +-- Fire async read-repair for any stale node
        |    +-- Return value to client
        |
        +-- [Async]
             +-- Return local store value immediately
             +-- Background: fan out to preference list peers,
                 find latest, repair stale nodes via putInternal()
```

### Hinted Handoff Recovery

```
Node 8081 goes down
        |
        +-- Writes to 8081 --> exception caught --> Hint stored in HintStore
        |
        |  [every 5 seconds]
        +-- HintedHandoffService: drain hints for 8081, try POST /internal/replicate
             +-- Still down? --> restore hints, retry next cycle
             +-- Back up?   --> deliver all hints --> node fully converges
```

### Anti-Entropy Sync

```
[every anti.entropy.delay.ms]
        |
        +-- Build local Merkle tree from store snapshot
        +-- For each peer:
             +-- GET /internal/merkle/hash/0  (root)
             +-- Hashes match? --> skip (entire keyspace in sync)
             +-- Differ? --> recurse into left/right subtrees
                  +-- Leaf bucket differs?
                       +-- GET /internal/merkle/bucket/{n}  (fetch peer's entries)
                       +-- Pull: peer newer + this node owns key --> putInternal()
                       +-- Push: local newer + peer owns key --> POST /internal/replicate
```

---

## Guarantees

| Property | Guarantee |
|---|---|
| Durability | All committed writes are in the WAL before response is sent |
| Fault tolerance | Survives up to N-1 node failures (RF=3 tolerates 1 failure in quorum; 2 in async) |
| Convergence | Read repair + hinted handoff + anti-entropy ensure eventual consistency after failures |
| Conflict resolution | Last-write-wins via Lamport timestamp (causal ordering, no wall-clock dependency) |
| Partition correctness | Anti-entropy only syncs keys within ring-designated owners; no key leaks to non-owners |
| No multi-key atomicity | Each key is handled independently; no cross-key transactions |

---

## Configuration

`src/main/resources/application.properties`:

```properties
replication.mode=async          # async | quorum
replication.factor=3            # number of nodes that own each key
write.quorum=2                  # minimum ACKs required (quorum mode)
anti.entropy.delay.ms=30000     # how often anti-entropy runs (ms)
Nodes=localhost:8080,localhost:8081,localhost:8082,localhost:8083
```

Per-node peer config (recommended — avoids self in node list):

```properties
# application-8080.properties
server.port=8080
Nodes=localhost:8081,localhost:8082,localhost:8083
```

Activate with `-Dspring.profiles.active=8080`.

---

## Running

**Build:**
```bash
mvn clean package -DskipTests
```

**Start a 4-node cluster:**
```bash
java -jar target/kv-store-0.0.1-SNAPSHOT.jar --spring.profiles.active=8080
java -jar target/kv-store-0.0.1-SNAPSHOT.jar --spring.profiles.active=8081
java -jar target/kv-store-0.0.1-SNAPSHOT.jar --spring.profiles.active=8082
java -jar target/kv-store-0.0.1-SNAPSHOT.jar --spring.profiles.active=8083
```

---

## API Reference

### Client endpoints

| Method | Path | Description |
|---|---|---|
| `PUT` | `/kv/put?key=&value=` | Write a key-value pair |
| `GET` | `/kv/get?key=` | Read a value (triggers quorum read or async repair) |

### Debug endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/kv/ring?key=` | Show preference list for a key |
| `GET` | `/kv/hints` | Show pending hint counts per down node |

### Internal endpoints (node-to-node only)

| Method | Path | Description |
|---|---|---|
| `POST` | `/kv/internal/replicate` | Follower write (timestamp already assigned by coordinator) |
| `GET` | `/kv/internal/get?key=` | Raw local read used for quorum reads and read repair |
| `GET` | `/kv/internal/merkle/hash/{nodeIndex}` | Merkle tree hash at given node index (root = 0) |
| `GET` | `/kv/internal/merkle/bucket/{bucketIndex}` | All key-value entries in a Merkle bucket (0–15) |

---

## Testing Scenarios

### Basic write and read
```bash
curl -X PUT "http://localhost:8080/kv/put?key=name&value=alice"
curl "http://localhost:8081/kv/get?key=name"
```

### Verify key ownership
```bash
curl "http://localhost:8080/kv/ring?key=name"
# e.g. ["localhost:8082","localhost:8080","localhost:8081"]
```

### Hinted handoff
```bash
# 1. Stop node 8081
# 2. Write via 8080
curl -X PUT "http://localhost:8080/kv/put?key=x&value=100"
# 3. Check hints buffered on coordinator
curl "http://localhost:8080/kv/hints"
# {"localhost:8081": 1}

# 4. Restart node 8081 — within 5 seconds hints are delivered
curl "http://localhost:8081/kv/get?key=x"
# 100
```

### Anti-entropy (simulate orphaned write)
```bash
# 1. Inject a key directly to one node only, bypassing replication
curl -X POST "http://localhost:8080/kv/internal/replicate" \
  -H "Content-Type: application/json" \
  -d '{"key":"orphan","value":"val","timestamp":1000}'

# 2. Confirm divergence via Merkle root
curl "http://localhost:8080/kv/internal/merkle/hash/0"   # differs
curl "http://localhost:8081/kv/internal/merkle/hash/0"   # differs

# 3. Wait one anti-entropy cycle (anti.entropy.delay.ms)
# 4. All owners of "orphan" now have it
curl "http://localhost:8081/kv/internal/get?key=orphan"
```

### Read repair
```bash
# Write via 8080, then read via 8082
# Quorum read queries all preference list nodes, picks the latest,
# and silently repairs any stale replica in the background
curl "http://localhost:8082/kv/get?key=x"
```

---

## Project Structure

```
src/main/java/com/aazim/kvstore/
├── clock/
│   └── LamportClock.java               Monotonic logical clock
├── config/
│   └── AppConfig.java                  RestTemplate with connect/read timeouts
├── controller/
│   └── KeyValueController.java         HTTP endpoints (client + internal + debug)
├── model/
│   ├── Hint.java                        Pending write for a down node
│   ├── LogEntry.java                    WAL record
│   ├── ReplicationRequest.java          Inter-node write payload
│   └── ValueEntry.java                  Value + Lamport timestamp
├── replication/
│   ├── AntiEntropyService.java          Merkle-tree-based background sync
│   ├── AsyncReplicationStrategy.java    Fire-and-forget replication
│   ├── AsyncSender.java                 @Async HTTP sender with hint fallback
│   ├── ConsistentHashRing.java          Virtual-node ring, preference lists
│   ├── HintStore.java                   In-memory hint buffer per node
│   ├── HintedHandoffService.java        Scheduled hint delivery (every 5s)
│   ├── MerkleTree.java                  16-bucket SHA-256 Merkle tree
│   ├── NodeResponse.java                Node + value pair for quorum reads
│   ├── QuorumReplicationStrategy.java   Synchronous quorum replication + read
│   ├── ReplicationStrategy.java         Strategy interface
│   └── ReplicationStrategyFactory.java  Selects strategy from config
├── scheduler/
│   └── CompactionService.java           Periodic WAL compaction (every 60s)
├── service/
│   └── KeyValService.java               Core read/write logic, ring ownership check
└── storage/
    ├── FileLogStore.java                WAL implementation (append + compact)
    ├── InMemoryStore.java               HashMap wrapper
    └── LogStore.java                    Storage interface
```

---

## Tech Stack

- Java 17
- Spring Boot 3
- Spring Web (RestTemplate for inter-node HTTP)
- Spring Scheduling (`@Scheduled`, `@Async`)
- Maven

---

## What's Not Implemented Yet

- **Gossip protocol** — membership is static config; nodes cannot discover each other or detect failures without polling; `addNode`/`removeNode` on the ring exist but are not wired to any membership event
- **Tombstones for deletes** — there is no delete operation; a deleted key must be overwritten; a node that missed the deletion will re-introduce the value during read repair or anti-entropy
- **Request routing** — a non-owner coordinator in async mode may return `null` for keys it does not hold locally; proper routing would transparently forward to a preference list node
