# KV-Cache: Distributed In-Memory Key-Value Store

## Overview

KV-Cache is a distributed, in-memory key-value store implemented using **Python** and **asyncio**. The system communicates over a **raw TCP protocol** (not HTTP) and supports basic key-value operations with additional features such as TTL (Time-To-Live), LRU eviction, sharding across multiple nodes, and Docker-based deployment.

The project is designed to demonstrate low-level networking, asynchronous server design, and basic distributed systems concepts.

## Architecture

The system consists of **multiple KV-Cache nodes**, each running the same async TCP server implementation. Every node:

* Runs an asyncio-based TCP server
* Maintains an in-memory `KVStore`
* Accepts persistent client connections
* Handles multiple concurrent clients without threads

Clients communicate with the server using a simple line-based text protocol over TCP. Each request is processed asynchronously and responded to immediately.

All nodes share the same codebase but behave differently based on environment variables such as `NODE_ID` and port configuration.

## Key-Value Store Design

Each node uses an in-memory `KVStore` with the following characteristics:

* **OrderedDict-based storage** for O(1) average operations
* **LRU eviction** when the maximum key limit is reached
* **TTL support** for automatic key expiration
* Lazy expiration during access and optional active cleanup

Supported operations:

* `PUT key value [ttl]`
* `GET key`
* `DELETE key`
* `EXISTS key`

## Sharding

Sharding is used to distribute keys across multiple nodes.

### Sharding Strategy

1. The key is hashed using **SHA-256**
2. The hash is converted to an integer
3. `shard_id = hash(key) % NUM_SHARDS`
4. Each shard is mapped to a **primary node** using a static mapping

Only the primary node for a shard is responsible for storing and serving that key.

### Request Forwarding

* When a node receives a request for a key it does not own, it **forwards** the request to the correct primary node.
* Forwarded requests are marked internally to avoid infinite forwarding loops.
* The response from the primary node is returned back to the client transparently.

This approach allows clients to connect to **any node** while still accessing the correct data.


## Replication

Replication is **not implemented** in this system.

Each shard has a **single primary node**, and all reads and writes for that shard are handled exclusively by that node.

This design choice:

* Simplifies consistency guarantees
* Avoids write conflicts
* Keeps the system easy to reason about

Replication could be added in the future by introducing secondary replicas and write propagation.

As a result, the system does not tolerate node failures and prioritizes simplicity over fault tolerance.

## Dockerization & Deployment

The application is fully containerized using Docker.

* A single Docker image is used for all nodes
* Node-specific behavior is controlled via environment variables
* A `docker-compose.yml` file starts a **3-node cluster**

Each node runs:

* One KV-Cache server
* On its own container
* On a shared Docker network

Health checks are performed using `nc` to validate server responsiveness.

## Challenges Faced

Some key challenges during implementation included:

* Implementing an **async TCP server** correctly using `asyncio`
* Managing persistent connections and clean shutdowns
* Handling malformed or empty client commands gracefully
* Implementing request forwarding without causing infinite loops
* Debugging Docker networking and port conflicts
* Testing TCP-based protocols using tools like `netcat`

Each challenge helped reinforce a deeper understanding of asynchronous programming and distributed system fundamentals.

## Conclusion

This project demonstrates a complete, working distributed key-value cache with:

* Async TCP networking
* In-memory storage with TTL and LRU
* Sharding across multiple nodes
* Dockerized multi-node deployment

The implementation prioritizes correctness, clarity, and simplicity while laying a strong foundation for future extensions such as replication or persistence.

Part B: Beyond the Basics (Research Write-up)
---------------------------------------------

### 1. Failure Detection


In a distributed key-value store, failure detection is typically done using heartbeat or ping-based mechanisms. Each node periodically sends messages to other nodes to indicate that it is alive. If a node does not respond within a configured timeout, it is suspected to have failed. However, this is not always reliable, because a node may simply be slow or experiencing temporary network delays rather than being fully crashed.

The main challenge is distinguishing between a slow node and a failed node. Treating slow nodes as failed can cause unnecessary failovers and instability, while waiting too long can reduce availability. Production systems therefore use probabilistic or consensus-based approaches instead of relying on a single observer.

Redis Cluster uses a gossip-based failure detection system. Nodes exchange information about other nodes’ health, and a node is marked as failed only when multiple nodes agree that it is unreachable. This reduces false positives and provides a more robust failure detection mechanism.

### 2. Handling Network Partitions

A network partition occurs when a cluster is split into multiple groups that cannot communicate with each other. For example, Node 1 may be able to talk to Node 2, but neither can reach Node 3. In such cases, each partition may have an incomplete view of the cluster state, which can lead to inconsistencies if all partitions continue accepting writes.

This situation can cause a split-brain problem, where multiple nodes believe they are the primary and accept conflicting writes. Preventing split-brain usually requires quorum-based decision making, where only the partition containing a majority of nodes is allowed to continue processing writes.

According to the CAP theorem, during a network partition a system must choose between consistency and availability. Redis Cluster prioritizes consistency by ensuring that only partitions with a majority of masters remain writable. Minority partitions stop accepting writes, preventing data divergence at the cost of reduced availability.

### 3. Consistency vs Availability Trade-offs

Synchronous replication ensures strong consistency by acknowledging a write only after it has been replicated to one or more replicas. This prevents data loss but increases write latency and reduces availability if replicas are slow or unreachable. In contrast, asynchronous replication allows the primary node to respond immediately, improving performance but risking data loss if the primary crashes before replicas are updated.

Redis Cluster primarily uses asynchronous replication to maximize availability and throughput. This means that in rare failure scenarios, recently written data may be lost. However, this trade-off is acceptable for many real-world use cases where performance and availability are more critical than strict consistency.

Redis also provides optional mechanisms, such as the WAIT command, which allow clients to request stronger durability guarantees when needed. This gives developers flexibility to balance consistency and availability based on application requirements.