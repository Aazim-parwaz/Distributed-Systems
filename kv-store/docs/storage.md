# Storage Layer

**Files:** `InMemoryStore.java`, `FileLogStore.java`, `CompactionService.java`, `LogStore.java`

---

## What It Does

Two-layer storage: an in-memory HashMap for fast reads/writes, backed by an append-only write-ahead log (WAL) on disk for durability and crash recovery.

### InMemoryStore

A `ConcurrentHashMap<String, ValueEntry>` wrapper. All reads and writes hit this first. Returns a defensive copy on `getAll()` for snapshot safety.

### FileLogStore (WAL)

Every committed write is appended to `kvstore_{port}.log` as a single CSV line:
```
key,value,timestamp
```
Appends are serialized with a `writeLock` to prevent torn writes. On startup, `recoverFromLogs()` replays all entries into `InMemoryStore`, applying last-write-wins by timestamp.

### CompactionService

Runs every 60 seconds. Rewrites the log file to contain only the latest value per key (the current in-memory snapshot), replacing the full append history with a compact snapshot. Uses an atomic temp-file-then-rename pattern to prevent log corruption on crash mid-compaction.

---

## Guarantees

- **Crash recovery**: the WAL is replayed on startup, restoring all committed writes up to the last append.
- **No torn writes**: the `writeLock` ensures appends are atomic at the application level.
- **Compaction safety**: the temp-file rename is atomic on the OS level — a crash mid-compaction leaves either the full old log or the new compact log, never a partial file.
- **Thread-safe reads**: `ConcurrentHashMap` allows concurrent reads without locking.

---

## Known Drawbacks

### 1. No fsync — writes are not truly durable
`BufferedWriter` flushes to the OS page cache but does not call `fsync`. A power loss after the append returns but before the OS flushes to disk means the write is lost — the WAL gives the appearance of durability without the guarantee.

### 2. Full log replay on startup
Recovery reads every line in the log file sequentially. On a large log (pre-compaction), startup is slow. There is no checkpoint or index to skip to the latest state.

### 3. Compaction locks writes for its entire duration
The `writeLock` is held for the full compaction — writing the temp file and renaming it. During this window (potentially seconds on a large store), all incoming writes block.

### 4. Plain CSV format — no schema, no binary encoding
The log format is `key,value,timestamp`. Values containing commas require special handling (currently `split(",", 3)` handles this), but values containing newlines would corrupt the log. There is no magic number, version header, or checksum per record to detect corruption.

### 5. No delete support
There is no tombstone mechanism. A deleted key must be overwritten with a sentinel value. If a node misses the overwrite and comes back online, anti-entropy or hinted handoff will re-introduce the old value as if it were the latest.

### 6. Single log file per node
All keys share one log file. On a large dataset, the file can grow very large between compaction cycles, and compaction itself is O(keys) in both time and disk I/O.

---

## Future Improvements

- **fsync on append**: call `FileDescriptor.sync()` (or use `FileChannel.force(true)`) after each write to guarantee durability on disk, not just in the page cache.
- **LSM tree / SSTables**: replace the single-file WAL with a proper LSM structure — in-memory memtable flushed to immutable SSTables on disk, with background compaction merging SSTables. This gives O(log N) reads, fast appends, and efficient range scans.
- **Per-record checksums**: add a CRC32 checksum to each log entry so corruption (partial writes, bit rot) can be detected and the recovery process can skip or report bad records.
- **Tombstones for deletes**: introduce a delete operation that writes a tombstone entry (`key,__DELETED__,timestamp`). Replay and anti-entropy both respect the tombstone, preventing deleted keys from being reintroduced.
- **Non-blocking compaction**: run the compaction snapshot on a separate copy of the store (or use a concurrent snapshot) so incoming writes are not blocked during the compaction write phase.
