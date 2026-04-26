# Distributed Key-Value Store

A Dynamo-style distributed key-value store built from scratch in Java (Spring Boot). Implements consistent hashing, quorum-based replication, read repair, hinted handoff, and a Lamport clock for conflict resolution — all without any external coordination service.

---

## Features

| Feature | Description |
|---|---|
| Consistent hashing | Keys are partitioned across nodes using a virtual-node ring (150 vnodes/node) |
| Two replication modes | `async` (high availability) or `quorum` (stronger consistency) — switchable via config |
| Lamport clock LWW | Monotonically increasing logical timestamps; last-write-wins conflict resolution |
| Read repair | Stale replicas are healed automatically on every read |
| Hinted handoff | Writes to down nodes are buffered and replayed when the node recovers |
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

A coordinator only stores data locally if it appears in the preference list. Writes are replicated exclusively to preference list nodes, not to the entire cluster.

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

---

## Guarantees

| Property | Guarantee |
|---|---|
| Durability | All committed writes are in the WAL before response is sent |
| Fault tolerance | Survives up to N-1 node failures (RF=3 tolerates 1 failure in quorum; 2 in async) |
| Convergence | Read repair + hinted handoff ensure eventual consistency after failures |
| Conflict resolution | Last-write-wins via Lamport timestamp (causal ordering, no wall-clock dependency) |
| No multi-key atomicity | Each key is handled independently; no cross-key transactions |

---

## Configuration

`src/main/resources/application.properties`:

```properties
replication.mode=async          # async | quorum
replication.factor=3            # number of nodes that own each key
write.quorum=2                  # minimum ACKs required (quorum mode)
Nodes=localhost:8080,localhost:8081,localhost:8082
```

Per-node peer config (recommended — avoids self in node list):

```properties
# application-8080.properties
server.port=8080
Nodes=localhost:8081,localhost:8082
```

Activate with `-Dspring.profiles.active=8080`.

---

## Running

**Build:**
```bash
mvn clean package -DskipTests
```

**Start a 3-node cluster (3 terminals):**
```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8080"
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8081"
mvn spring-boot:run -Dspring-boot.run.arguments="--server.port=8082"
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
│   ├── AsyncReplicationStrategy.java    Fire-and-forget replication
│   ├── AsyncSender.java                 @Async HTTP sender with hint fallback
│   ├── ConsistentHashRing.java          Virtual-node ring, preference lists
│   ├── HintStore.java                   In-memory hint buffer per node
│   ├── HintedHandoffService.java        Scheduled hint delivery (every 5s)
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

- **Gossip protocol** — membership is static config; nodes cannot discover each other or detect failures without polling
- **Anti-entropy / Merkle trees** — hinted handoff covers outages up to 1 hour; longer outages or silent corruption require a full replica comparison and sync
- **Tombstones for deletes** — there is no delete operation; a deleted key must be overwritten; a node that missed the deletion will re-introduce the value during read repair
- **Request routing** — a non-owner coordinator in async mode may return `null` for keys it does not hold locally; proper routing would transparently forward to a preference list node
