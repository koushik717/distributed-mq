# Distributed Message Queue

A production-grade distributed message queue built from scratch in Java — inspired by Apache Kafka's core architecture. Features partitioned append-only logs, consumer groups with offset tracking, ISR-based replication, and Prometheus metrics.

---

## 🚀 Live Demo

**Frontend Dashboard:** [mq-dashboard-pi.vercel.app](https://mq-dashboard-pi.vercel.app)

| Broker | Status | URL |
|--------|--------|-----|
| Broker 1 | Live | http://157.230.83.134:9095 |
| Broker 2 | Live | http://157.230.83.134:9093 |
| Broker 3 | Live | http://157.230.83.134:9094 |

### Try it yourself:
```bash
# Produce a message
curl -X POST http://157.230.83.134:9095/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{"key":"user-123","value":"order-placed"}'

# Consume messages
curl "http://157.230.83.134:9095/topics/orders/messages?offset=0&limit=10"

# List topics
curl http://157.230.83.134:9095/topics

# Prometheus metrics: http://157.230.83.134:9099
```

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   PRODUCER API                       │
│         POST /topics/{topic}/messages                │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│              BROKER CLUSTER (3 nodes)                │
│                                                      │
│  Topic: "orders"  →  3 Partitions                   │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐    │
│  │ Partition 0│  │ Partition 1│  │ Partition 2│    │
│  │ Leader:N1  │  │ Leader:N2  │  │ Leader:N3  │    │
│  │ [msg0]     │  │ [msg0]     │  │ [msg0]     │    │
│  │ [msg1]     │  │ [msg1]     │  │ [msg1]     │    │
│  │ [msg2]...  │  │ [msg2]...  │  │ [msg2]...  │    │
│  └────────────┘  └────────────┘  └────────────┘    │
│         │ replication                               │
│  Each partition replicated to 2 follower brokers    │
└──────────────────┬──────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────┐
│              CONSUMER GROUP API                      │
│  GET /consumer-groups/{groupId}/poll                 │
│                                                      │
│  Consumer Group "payments-service":                  │
│  Consumer 1 → Partition 0                           │
│  Consumer 2 → Partition 1                           │
│  Consumer 3 → Partition 2                           │
└─────────────────────────────────────────────────────┘
```

## Core Features

### Append-Only Log Storage
Each partition is backed by a binary log file using `java.nio.FileChannel` for maximum I/O performance:
- **O(1) writes** — messages appended to end of file
- **Binary format**: `[8B offset][8B timestamp][4B keyLen][key][4B valLen][value]`
- **Crash recovery** — log is replayed on startup to rebuild in-memory index
- Exactly the same storage model Kafka uses internally

### Partitioning & Key-Based Routing
- `MurmurHash3(key) % partitionCount` → same key always routes to same partition
- Guarantees **per-key ordering** — all events for a user processed in order
- Null keys distributed via round-robin across partitions

### Consumer Groups
- **Same group = load balancing** — each consumer gets exclusive partition subset
- **Different groups = fan-out** — both groups get all messages independently
- Range-based partition assignment with automatic rebalance
- **Offset tracking** persisted to disk per partition per group
- **Consumer lag** metric: `latest_offset - committed_offset`

### Replication & Leader Election
- **Leader-follower model** — leader handles all reads/writes per partition
- **ISR (In-Sync Replicas)** — tracks which followers are caught up
- Messages committed only when all ISR acknowledge (durability guarantee)
- **Automatic leader election** from ISR on failure (prevents data loss)
- Heartbeat-based failure detection

### Prometheus Metrics
```
mq_messages_produced_total{topic, partition}
mq_messages_consumed_total{topic, partition}
mq_bytes_written_total{topic}
mq_consumer_lag{topic, partition, consumer_group}
mq_replication_lag{topic, partition, follower}
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Java 17 |
| Framework | Spring Boot 3.4.3 |
| Storage | `java.nio.FileChannel` (append-only binary log) |
| Hash | MurmurHash3 (same as Kafka) |
| Metrics | Micrometer + Prometheus |
| Build | Gradle |
| Container | Docker + Docker Compose |
| Cluster | 3-broker Docker Compose setup |

## API Reference

### Producer API
```bash
# Publish a message
curl -X POST http://localhost:8080/topics/orders/messages \
  -H "Content-Type: application/json" \
  -d '{"key":"user-123","value":"order-placed"}'

# Response: {"topic":"orders","partition":2,"offset":0,"timestamp":1234567890}
```

### Consumer API
```bash
# Read messages from a partition
curl "http://localhost:8080/topics/orders/messages?offset=0&limit=100&partition=0"

# Response: {"messages":[...],"count":100,"nextOffset":100}
```

### Consumer Group API
```bash
# Join a consumer group
curl -X POST "http://localhost:8080/consumer-groups/payments/join?topic=orders&consumerId=c1"

# Poll messages (from assigned partitions)
curl "http://localhost:8080/consumer-groups/payments/poll?topic=orders&consumerId=c1&limit=100"

# Commit processed offsets
curl -X POST http://localhost:8080/consumer-groups/payments/commit \
  -H "Content-Type: application/json" \
  -d '{"consumerId":"c1","topic":"orders","offsets":{"0":150,"1":200}}'

# Check consumer group status & lag
curl "http://localhost:8080/consumer-groups/payments/status?topic=orders"
```

### Metadata API
```bash
# List topics
curl http://localhost:8080/topics

# Create a topic
curl -X POST "http://localhost:8080/topics/orders?partitions=6"

# Get topic metadata
curl http://localhost:8080/topics/orders/metadata
```

## Running Locally

### Single Broker
```bash
./gradlew bootRun
```

### 3-Broker Cluster
```bash
docker compose up -d

# Publish through broker 1
curl -X POST http://localhost:9092/topics/orders/messages \
  -d '{"key":"user-1","value":"order-placed"}'

# Read from broker 2
curl "http://localhost:9093/topics/orders/messages?offset=0&limit=10"

# Prometheus metrics: http://localhost:9090
```

### Run Tests
```bash
./gradlew test
```

### Run Benchmark
```bash
./gradlew bootRun &
java -cp build/libs/distributed-mq-1.0.0.jar \
  com.koushik.mq.benchmark.BenchmarkRunner \
  --producers 10 --messages 100000 --size 1024
```

## Design Decisions

1. **FileChannel over RandomAccessFile** — FileChannel provides direct OS-level I/O, bypassing Java's stream abstraction. This is the same approach Kafka uses for its log segments.

2. **Binary format over JSON** — Compact binary encoding minimizes disk I/O and parsing overhead. Each message is stored with fixed-size headers for O(1) offset calculation.

3. **In-memory offset index** — Rather than scanning the entire log file, we maintain an `ArrayList<Long>` mapping offset → file position for O(1) seeks. Rebuilt on startup via log replay.

4. **ISR-based commit** — Messages are only considered "committed" when all In-Sync Replicas acknowledge. This prevents data loss during leader failover, at the cost of slightly higher latency.

5. **Range-based partition assignment** — Simple, deterministic assignment strategy. Each consumer gets a contiguous range of partitions, which is the default strategy in Kafka.

## Project Structure
```
src/main/java/com/koushik/mq/
├── MqApplication.java           # Spring Boot entrypoint
├── api/
│   ├── ProducerController.java   # POST /topics/{topic}/messages
│   ├── ConsumerController.java   # GET  /topics/{topic}/messages
│   ├── MetadataController.java   # GET  /topics, /topics/{topic}/metadata
│   ├── ConsumerGroupController.java  # Consumer group join/poll/commit
│   └── ReplicationController.java    # Internal replication endpoints
├── config/
│   └── MqConfig.java             # Broker configuration
├── storage/
│   ├── Message.java              # Message record
│   ├── Partition.java            # Append-only log (FileChannel)
│   ├── Topic.java                # Partitioned topic
│   └── TopicManager.java         # Topic registry
├── consumer/
│   ├── ConsumerGroup.java        # Offset tracking, partition assignment
│   └── ConsumerGroupManager.java # Group registry
├── replication/
│   ├── BrokerRegistry.java       # Peer discovery, heartbeats
│   ├── ReplicationManager.java   # ISR tracking, replication loop
│   └── LeaderElection.java       # Failure detection, leader election
├── metrics/
│   └── MqMetrics.java            # Prometheus counters
├── util/
│   └── MurmurHash3.java          # Hash for partition routing
└── benchmark/
    └── BenchmarkRunner.java      # Throughput benchmarking tool
```
