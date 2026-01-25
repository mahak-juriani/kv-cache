# KV-Cache: In-Memory Key-Value Store

## Overview

KV-Cache is a high-performance, in-memory key-value cache server implemented in **Python** using **asyncio**.
The server communicates over **raw TCP sockets** using a custom text-based protocol (not HTTP) and supports **TTL-based expiration** and **LRU eviction**.

The system is designed to efficiently handle a large number of concurrent clients with low latency and high throughput.

---

## Features

* Raw TCP socket communication (custom protocol)
* Asynchronous server using `asyncio`
* Persistent client connections
* Commands: `PUT`, `GET`, `DELETE`, `EXISTS`, `QUIT`
* TTL (Time-To-Live) support
* Lazy and active expiration of keys
* LRU (Least Recently Used) eviction policy
* Dockerized deployment
* AWS EC2 deployment with load testing

---

## Protocol Specification

### Request Format

```
<COMMAND> <ARGS...>\n
```

### Supported Commands

#### PUT

```
PUT <key> <value> [ttl]
```

* Stores or updates a key-value pair
* `ttl` is optional (in seconds)
* `ttl = 0` means no expiration

Response:

```
OK stored
```

#### GET

```
GET <key>
```

Response:

* If found: `OK <value>`
* If not found or expired: `ERROR key not found`

#### DELETE

```
DELETE <key>
```

Response:

* If deleted: `OK deleted`
* If not found: `ERROR key not found`

#### EXISTS

```
EXISTS <key>
```

Response:

* If exists and not expired: `OK 1`
* Otherwise: `OK 0`

#### QUIT

```
QUIT
```

Closes the connection gracefully.

---

## Design Choices & Optimizations

### 1. Async TCP Server

* Implemented using `asyncio.start_server`
* Each client is handled as an independent coroutine
* Enables high concurrency without thread overhead

### 2. O(1) Cache Operations

* Uses `collections.OrderedDict` for internal storage
* Provides:

  * O(1) `put`, `get`, `delete`, `exists`
  * Efficient LRU reordering using `move_to_end()`

### 3. TTL (Time-To-Live)

* Each key stores an expiration timestamp
* **Lazy expiration**: expired keys are removed on access
* **Active expiration**: implemented using `cleanup_expired()`

### 4. LRU Eviction

* When the cache reaches maximum capacity, the least recently used key is evicted
* Accessing or updating a key moves it to the most recently used position
* Eviction is O(1)

### 5. Docker & AWS Deployment

* Docker image exposes port `7171`
* Server starts automatically when the container runs
* Deployed on AWS EC2 and validated with load testing

---

## Assumptions

* Keys and values do not contain whitespace
* Maximum key length: 256 characters
* Maximum value length: 256 characters
* TTL is a non-negative integer
* Thread safety is not required (single-process asyncio server)

---

## Build & Run Instructions

### Build Docker Image

```bash
docker build -t mahakjuriani/kv-cache:latest .
```

### Run with Docker

```bash
docker run -p 7171:7171 mahakjuriani/kv-cache:latest
```

The server starts automatically and listens on port `7171`.

---

## Manual Testing

### Using Netcat

```bash
nc localhost 7171
PUT mykey myvalue
GET mykey
EXISTS mykey
DELETE mykey
QUIT
```

---

## AWS Deployment & Load Testing

Deployment and load testing were performed using the provided AWS scripts.

```bash
./scripts/aws.sh create
./scripts/aws.sh deploy
./scripts/aws.sh test
```

---

## Load Test Results Summary (AWS)

The load test was executed from a client EC2 instance against the server EC2 instance.

**Results:**

* Total Requests: 100,000
* Requests/second: ~11,600
* Average latency: ~8.5 ms
* P99 latency: ~18.7 ms
* Error rate: 0%

All required performance thresholds were successfully met.

The full load test report is available at:

```
results/load_test_results.json
```

---

## Docker Requirements Compliance

* Exposes port `7171`
* Runs with:

  ```bash
  docker run -p 7171:7171 mahakjuriani/kv-cache:latest
  ```
* Starts the server automatically on container start
* Publicly accessible on Docker Hub

---

## Repository & Submission Details

* GitHub repository is **private**
* Read access provided to:

  * `AgarwalPragy`
  * `anshumansingh`
* Docker image is public on Docker Hub
* Load test results included in `results/` directory

---

## Author

**Mahak Juriani**
