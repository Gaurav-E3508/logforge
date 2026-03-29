# LogForge — Distributed Log Intelligence Platform

A production-grade distributed logging system built from scratch using
Java 17 and Spring Boot 3. Designed to ingest, process, store, and
search millions of log events in real time across multiple microservices.

## Architecture Overview
```
Your Applications (Payment, Auth, Orders...)
         │
         ▼  [Log Agent SDK — async ring buffer]
    Apache Kafka  ←── central event bus
         │
    ┌────┴────┐
    ▼         ▼
Ingestion   Stream
Service    Processor
(pipeline) (windows)
    │         │
    ▼         ▼
 3-Tier    Alert
 Storage   Engine
(Redis/    (EWMA +
Mongo/     fanout)
Disk)
    │
    ▼
Query Service + WebSocket Live Tail
```

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Java 17 |
| Framework | Spring Boot 3 |
| Message Bus | Apache Kafka |
| Hot Storage | Redis (1 hour TTL) |
| Warm Storage | MongoDB (30 days) |
| Cold Storage | Custom segment files (1 year) |
| Coordination | Apache ZooKeeper |
| Containerization | Docker + Docker Compose |

## DSA & System Design Concepts Implemented

- **Bloom Filter** — log deduplication in the ingestion pipeline
- **Inverted Index with Posting Lists** — full-text search without Elasticsearch
- **Sliding Window** — real-time error rate aggregation
- **Ring Buffer (Disruptor)** — non-blocking async log capture in the SDK
- **K-way Merge (Min-Heap)** — unified search across 3 storage tiers
- **EWMA** — statistical anomaly detection for alert engine
- **Consistent Hashing** — storage routing
- **AST-based Query Parser** — mini query language (`level:ERROR AND service:payment`)

## Modules

| Module | Description | Status |
|---|---|---|
| `logforge-common` | Shared schemas and utilities | ✅ Done |
| `log-agent-sdk` | Auto-capture SDK for Spring Boot apps | 🔄 In Progress |
| `ingestion-service` | Kafka consumer + processing pipeline | 🔄 In Progress |
| `stream-processor` | Sliding window aggregations | ⏳ Planned |
| `index-engine` | Custom inverted index engine | ⏳ Planned |
| `storage-service` | Hot/warm/cold tiering | ⏳ Planned |
| `alert-engine` | Anomaly detection + notifications | ⏳ Planned |
| `query-service` | REST API + WebSocket live tail | ⏳ Planned |

## Running Locally

**Prerequisites:** Java 17, Maven 3.9+, Docker Desktop
```bash
# 1. Start all infrastructure (Kafka, Redis, MongoDB, ZooKeeper)
docker compose up -d

# 2. Build all modules
mvn clean install -DskipTests

# 3. Kafka UI dashboard
open http://localhost:8090
```

## Build Status
```
✅ Project scaffold — all 8 modules wired in Maven multi-module structure
✅ Docker Compose — Kafka, ZooKeeper, Redis, MongoDB running locally
🔄 Step 2 — LogEvent schema + Kafka topology (in progress)
```