# Distributed Systems

A collection of hands-on projects exploring the design and implementation of distributed systems. This repository is focused on building real-world systems from scratch to understand core concepts like consistency, replication, fault tolerance, and scalability.

---

## 🚀 Purpose

This repo is my playground for learning and implementing distributed systems concepts through practical projects. Instead of just theory, each project here dives into system design, trade-offs, and real implementation details.

---

## 📦 Projects

### 1. Key-Value Store (KV Store)

A distributed key-value store designed to explore fundamental distributed system concepts.

**Features:**

* Consistent hashing for partitioning
* Replication across nodes
* Quorum-based reads and writes
* Fault tolerance and node recovery
* Pluggable consistency models (strong/eventual)

**Concepts Covered:**

* Partitioning (Consistent Hashing)
* Replication strategies
* Quorum protocols (R/W/N)
* Failure handling
* Gossip / membership (planned)
* Anti-entropy (planned)

---

## 🧠 Concepts You’ll Find Here

* Consistent Hashing
* Replication & Data Distribution
* Leaderless Architectures
* Quorum-based Consistency
* Eventual vs Strong Consistency
* Fault Tolerance
* Anti-Entropy & Repair (Merkle Trees)
* Distributed Coordination (upcoming)
* Consensus Algorithms (Raft / Paxos – upcoming)

---

## 🛠 Tech Stack

* **Languages:** Java, Python 
* **Frameworks:** Spring Boot (for service layer)
* **Data Processing:** Custom in-memory + disk-backed storage
* **Cloud/Infra (Planned):** Docker, Kubernetes, AWS

---

## 📈 Roadmap

* [x] KV Store
* [ ] Add persistent storage (LSM / SSTables)
* [ ] Implement gossip-based membership
* [x] Add hinted handoff
* [ ] Anti-entropy using Merkle Trees
* [ ] Observability (metrics, logging, tracing)
* [ ] Benchmarking and load testing
* [ ] Multi-datacenter replication
* [ ] Raft-based consensus module
* [ ] Distributed task queue

---

## 📚 Learning Philosophy

This repository is built with a **“learn by building”** approach:

* Start simple
* Add complexity incrementally
* Understand trade-offs at each step
* Focus on system behavior under failure

---

## 🤝 Contributions

This is primarily a personal learning repo, but suggestions, discussions, and improvements are always welcome. open an issue and we can work on that.

---

## 📌 Notes

* Each project will have its own detailed README explaining design decisions and trade-offs.
* Code prioritizes clarity and learning over production-level optimization (initially).

---

## ⭐ Why This Repo?

This repo demonstrates:

* Ability to design scalable systems
* Strong fundamentals in distributed computing
* Practical implementation of complex concepts

---

## 🔗 Future Work

This repo will evolve into a full distributed systems toolkit covering:

* Storage systems
* Streaming systems
* Coordination services
* Distributed scheduling

---

**Stay tuned. Building systems > just reading about them.**
