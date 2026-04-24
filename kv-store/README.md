# 🧠 Distributed KV Store (Spring Boot)

A lightweight distributed key-value store built using Spring Boot.
This project is designed to explore distributed system concepts like multi-node communication, replication, and fault tolerance.

---

## 🚀 Features

* REST-based **GET / PUT APIs**
* Multi-node setup (e.g., ports `8080`, `8081`, `8082`)
* Inter-node communication using HTTP
* In-memory storage (for simplicity)
* Extensible architecture for replication & clustering

---

## 🏗️ Tech Stack

* Java 17+
* Spring Boot
* Spring Web
* Maven / Gradle

---

## ⚙️ Getting Started

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd <repo-name>
```

---

### 2. Build the Project

Using Maven:

```bash
mvn clean install
```

---

### 3. Run Multiple Nodes

Run the application on different ports:

```bash
# Node 1
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8080

# Node 2
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081

# Node 3
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8082
```

---

## 📡 API Endpoints

### PUT (Store a value)

```bash
curl -X PUT "http://localhost:8080/kv/put?key=foo&value=bar"
```

---

### GET (Retrieve a value)

```bash
curl "http://localhost:8080/kv/get?key=foo"
```

---

## 🔁 Multi-node Example

```bash
for port in 8080 8081 8082; do
  curl -X PUT "http://localhost:$port/kv/put?key=test&value=hello"
done
```

---

## 🏛️ Architecture Overview

* Each Spring Boot instance acts as a **node**
* Nodes communicate via REST APIs
* Requests can be:

  * handled locally, or
  * forwarded to other nodes
* Future enhancements:

  * replication strategies
  * leader election
  * consistency models

---

## 🧪 Running Tests

```bash
mvn test
```

(Add more tests as the project evolves)

---

## 🤝 Contributing

We welcome contributions! Follow these steps to keep the project clean and maintainable.

---

### 🧩 1. Pick or Create an Issue

* Browse existing issues
* Or create a new one with:

  * clear description
  * expected behavior
  * scope of changes

---

### 🌱 2. Create a Feature Branch

```bash
git checkout -b feature/<short-description>
```

Examples:

* `feature/kv-put-endpoint`
* `feature/node-replication`
* `feature/error-handling`

---

### 💻 3. Development Guidelines

* Follow standard Spring Boot structure:

  * `controller` → API layer
  * `service` → business logic
  * `repository` → storage layer (in-memory for now)
* Keep methods small and readable
* Write meaningful commit messages:

```bash
git commit -m "add PUT endpoint"
git commit -m "implement multi-node forwarding"
```

---

### 🔗 4. Link Commits to Issues

```bash
git commit -m "implement GET endpoint (closes #12)"
```

---

### 🔍 5. Open a Pull Request

* Clearly describe:

  * what you built
  * how to test it
* Link related issue
* Keep PRs focused (one feature per PR)

---

### ✅ 6. Merge Criteria

* Application builds successfully
* No breaking changes
* Code is readable and structured
* Basic manual testing done

---

## 📌 Project Workflow

We use GitHub Projects (Kanban board):

* 🧠 Backlog → ideas
* 📌 Todo → ready tasks
* 🚧 In Progress → active work
* 🧪 Testing → validation
* ✅ Done → completed

---

## 🧠 Contribution Ideas

* Add validation for inputs
* Improve exception handling
* Add logging (Spring Boot logging)
* Implement replication between nodes
* Add health check endpoints
* Introduce persistent storage
* Write unit and integration tests

---

## 🛣️ Roadmap

* [ ] Basic KV APIs
* [ ] Multi-node communication
* [ ] Replication
* [ ] Leader election
* [ ] Fault tolerance
* [ ] Consistency guarantees

---

## ⚠️ Guidelines

* Do not commit secrets or credentials
* Keep code modular and clean
* Prefer clarity over complexity
* Follow REST best practices

---

## 📄 License

(Add your license here, e.g., MIT)

---

## 🙌 Acknowledgements

Inspired by distributed systems and in-memory data stores like Redis.
