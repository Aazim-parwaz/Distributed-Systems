# Consistent Hash Ring

**File:** `ConsistentHashRing.java`

---

## What It Does

Partitions keys across nodes so that adding or removing a node remaps only ~1/N keys instead of the entire keyspace. Provides an ordered preference list of N nodes for any key, used by both the replication strategies and anti-entropy.

### Ring Construction

- Each physical node gets 150 virtual nodes (vnodes) by hashing `node#0` through `node#149` with MD5.
- All vnode hashes are stored in a sorted `TreeMap<Long, String>` (hash position → physical node).
- Total ring entries: `physical_nodes × 150`.

### Key Lookup (`getPreferenceList`)

1. Hash the key with MD5.
2. Walk clockwise from that position using `TreeMap.tailMap()`.
3. Collect distinct physical nodes until the preference list has `count` entries.
4. Wrap around to the start of the ring if needed.

### Remapping on topology change

When a node is added: it inserts 150 positions across the ring and steals only the keys in those arcs from existing neighbors. Expected remapping: ~1/(N+1) of all keys.

When a node is removed: its 150 positions are removed and their arcs fall to the next clockwise node. Expected remapping: ~1/N of all keys.

Virtual nodes spread each physical node's arcs across the entire ring, so the load shed/absorbed during a topology change is distributed evenly rather than falling on a single neighbor.

---

## Guarantees

- A key always maps to the same ordered preference list given the same ring state.
- Adding or removing a node only remaps keys whose hash position falls in the affected arcs (~1/N).
- 150 vnodes per node provides approximately uniform load distribution across physical nodes.
- The preference list always returns distinct physical nodes — no duplicate owners.

---

## Known Drawbacks

### 1. Ring is static — no dynamic membership
The ring is built once at startup from the `${Nodes}` config. `addNode` and `removeNode` exist as methods but are never called at runtime. A topology change requires a config update and full cluster restart. There is no gossip or membership protocol to detect nodes joining or leaving.

### 2. No data migration on topology change
When a node is added or removed, the ring remaps ~1/N keys to different owners. The data does not automatically move — the new owner will have an empty store for those keys until anti-entropy syncs them. During this window, reads routed to the new owner return `null`.

### 3. MD5 hash produces a 32-bit space
The hash is truncated to 32 bits (4 bytes combined from the MD5 digest). With many keys, hash collisions on the ring are more likely than with a 64-bit or 128-bit hash. In practice this is fine for small clusters, but reduces theoretical ring capacity.

### 4. Virtual node count is hardcoded
`VIRTUAL_NODES = 150` is a constant. Heterogeneous nodes (more RAM, more CPU) cannot be given more vnodes to absorb proportionally more load without a code change.

### 5. No awareness of rack or zone
All nodes are treated as equivalent. There is no rack-aware placement — replicas could all land on the same physical machine or availability zone, eliminating fault isolation.

---

## Future Improvements

- **Gossip-based dynamic membership**: integrate a gossip protocol so nodes can join and leave without a cluster restart. Wire gossip membership events to `addNode` / `removeNode` on the ring.
- **Automatic data migration**: when the ring changes, trigger targeted anti-entropy only for the remapped key ranges rather than a full scan, so new owners catch up quickly.
- **Configurable vnode count per node**: allow higher-capacity nodes to be assigned more vnodes via config, enabling proportional load distribution in heterogeneous clusters.
- **Rack/zone-aware placement**: extend `getPreferenceList` to ensure replicas are spread across distinct racks or availability zones for better fault isolation.
- **64-bit hash**: replace the 32-bit MD5 truncation with a full 64-bit hash (e.g. MurmurHash3) for a larger, more uniform ring space.
