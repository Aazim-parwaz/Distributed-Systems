# Design Documentation

One document per service. Each covers current behaviour, guarantees, known drawbacks, and future improvements.

| Document | Service(s) |
|---|---|
| [anti-entropy.md](anti-entropy.md) | `AntiEntropyService`, `MerkleTree` |
| [quorum-replication.md](quorum-replication.md) | `QuorumReplicationStrategy` |
| [async-replication.md](async-replication.md) | `AsyncReplicationStrategy` |
| [hinted-handoff.md](hinted-handoff.md) | `HintedHandoffService`, `HintStore`, `AsyncSender` |
| [gossip.md](gossip.md) | `GossipService`, `MemberInfo`, `NodeState` |
| [consistent-hashing.md](consistent-hashing.md) | `ConsistentHashRing` |
| [storage.md](storage.md) | `FileLogStore`, `InMemoryStore`, `CompactionService` |
