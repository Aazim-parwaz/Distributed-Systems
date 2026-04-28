# Anti-Entropy Service

**Files:** `AntiEntropyService.java`, `MerkleTree.java`

---

## What It Does

Runs as a background scheduler on every node. Periodically compares the full local store state with each peer using a Merkle tree and repairs any divergence — without waiting for a read to trigger it.

### Merkle Tree Structure

- 16 leaf buckets. Keys distributed by `Math.abs(key.hashCode()) % 16`.
- Each leaf hashes its sorted `key:value:timestamp` pairs with SHA-256.
- Internal nodes hash their two children bottom-up.
- Full tree has 31 nodes in a flat array (root at index 0).
- Two nodes with identical data always produce the same root hash.

### Sync Protocol

1. Build a local Merkle tree from a store snapshot.
2. Fetch the peer's root hash (`GET /internal/merkle/hash/0`).
3. If roots match — entire keyspace is in sync, stop.
4. If roots differ — recurse into left and right subtrees, skipping matching ones.
5. On a diverged leaf bucket: fetch the peer's bucket entries (`GET /internal/merkle/bucket/{n}`), then sync bidirectionally.
6. **Pull**: if peer has a newer timestamp for a key this node owns → `putInternal()`.
7. **Push**: if local has a newer timestamp for a key the peer owns → `POST /internal/replicate`.

### Ring Ownership Check

Before pulling or pushing any key, the preference list for that key is checked against the ring. A node will not accept a key it does not own, and will not push a key to a peer that does not own it. This preserves consistent hashing partitioning.

---

## Guarantees

- All divergence is eventually detected and repaired, regardless of how it occurred.
- Repairs are bidirectional — both stale-local and stale-peer cases are handled in one round.
- Ring ownership is respected — no key leaks outside its designated preference list.
- Convergence is bounded by `anti.entropy.delay.ms` (default 30s).

---

## Known Drawbacks

### 1. Peer rebuilds Merkle tree on every request
The controller builds a fresh `MerkleTree` for every `GET /merkle/hash/{n}` call. If the initiator traverses 10 nodes, the peer builds the tree 10 times — each O(number of keys). On large stores this is expensive.

### 2. No snapshot consistency across a round
The tree is rebuilt fresh per request. A write arriving between the hash check and the bucket fetch means the initiator is comparing hashes from snapshot S1 with bucket data from snapshot S2. The round is internally inconsistent, which can cause unnecessary syncs or missed repairs within one round (corrected in the next round).

### 3. Chatty HTTP protocol
Worst case per peer:
- Up to 31 `GET /merkle/hash/{n}` requests (full tree traversal)
- Up to 16 `GET /merkle/bucket/{n}` requests
- N × `POST /internal/replicate` (one per stale key)

With 3 peers in a 4-node cluster, a fully diverged round can produce 150+ HTTP calls.

### 4. Sequential tree traversal
Children of a differing internal node are checked sequentially. The left subtree must fully resolve before the right subtree starts.

### 5. Push is one HTTP call per key
Each stale key in a diverged bucket produces a separate `POST /internal/replicate`. A bucket with 100 stale keys causes 100 HTTP requests.

---

## Future Improvements

### Short term
- **Cache the Merkle tree server-side** with a short TTL (e.g. 5s). All requests within one round hit the same snapshot. Fixes both the rebuild cost and the snapshot consistency issue.
- **Single endpoint returning all 31 hashes** (`GET /merkle/tree`). Reduces the hash-comparison phase from up to 31 requests to 1.
- **Batch push endpoint** (`POST /internal/replicate/batch` accepting `List<ReplicationRequest>`). One call per diverged bucket instead of one per key.

### Medium term
- **Parallel child traversal** — fan both children out simultaneously using `CompletableFuture` at each internal node.
- **Scoped anti-entropy** — only compare keys in the ring ranges shared by both nodes, rather than the full keyspace.

### Long term
- **Replace HTTP with gRPC** — binary framing, multiplexing over a single persistent connection, streaming for bucket sync. HTTP/JSON is the wrong tool for a tight internal sync loop.
- **Gossip-triggered anti-entropy** — instead of a fixed timer, trigger a round when the gossip layer detects a node has just recovered, for faster convergence after outages.
