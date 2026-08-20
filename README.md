# Log Ingestion Service

<p align="center">
  <strong>High-performance log ingestion and query service designed for reliable, scalable, and efficient log processing.</strong>
</p>

<p align="center">
  Built with TypeScript, Node.js, Fastify, PostgreSQL, Zod, and Docker.
</p>

<p align="center">

![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?style=for-the-badge\&logo=typescript\&logoColor=white)
![Node.js](https://img.shields.io/badge/Node.js-22+-339933?style=for-the-badge\&logo=node.js\&logoColor=white)
![Fastify](https://img.shields.io/badge/Fastify-5.x-000000?style=for-the-badge\&logo=fastify\&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16+-4169E1?style=for-the-badge\&logo=postgresql\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge\&logo=docker\&logoColor=white)
![Zod](https://img.shields.io/badge/Zod-Validation-3E67B1?style=for-the-badge)

</p>

---

## Overview

**Log Ingestion Service** is a backend system designed to ingest, store, query, and aggregate large volumes of application logs.

The project focuses on practical backend engineering challenges associated with high-volume workloads, including:

* High-throughput log ingestion
* Batch processing
* Input validation
* Partial-batch error handling
* PostgreSQL bulk inserts
* Concurrent ingestion
* Cursor-based pagination
* JSONB attributes
* Full-text message search
* Time-based aggregation
* Pre-aggregated one-minute rollups
* Database indexing
* Transactional writes
* Deadlock retry handling
* Data retention
* Dockerized deployment
* Load and stress testing

The implementation is designed to prioritize both **performance and correctness**, rather than optimizing throughput at the cost of data integrity.

---

# Benchmark

The service was evaluated using the project benchmark suite under a high-throughput workload.

## Overall Score

| Category    |          Score | Result               |
| ----------- | -------------: | -------------------- |
| Correctness |  **15.0 / 15** | 15/15 checks passed  |
| Performance |  **34.5 / 50** | 14,636 logs/sec      |
| Queries     |  **13.9 / 15** | 59 ms aggregate p95  |
| Reliability |  **20.0 / 20** | 4/4 scenarios passed |
| **Total**   | **83.5 / 100** |                      |

## Key Metrics

| Metric            |              Result |
| ----------------- | ------------------: |
| Throughput        | **14,636 logs/sec** |
| Error Rate        |            **0.0%** |
| Ingestion p95     |        **1,150 ms** |
| Aggregate p95     |           **59 ms** |
| Correctness       |    **15/15 checks** |
| Query Consistency |             **4/4** |
| Reliability       |   **4/4 scenarios** |
| Overall Score     |      **83.5 / 100** |

### Score Breakdown

```mermaid
xychart-beta
    title "Benchmark Score"
    x-axis ["Correctness", "Performance", "Queries", "Reliability"]
    y-axis "Score" 0 --> 50
    bar [15, 34.5, 13.9, 20]
```

### Performance Summary

The benchmark demonstrates that the service can sustain approximately:

**14,636 logs per second**

while maintaining:

* **0.0% error rate**
* **15/15 correctness checks**
* **4/4 reliability scenarios**
* **59 ms aggregate p95**
* **1,150 ms ingestion p95**

These results demonstrate strong correctness and reliability together with high ingestion throughput and efficient aggregation queries.

> Benchmark results are environment-dependent and can vary based on CPU, memory, PostgreSQL configuration, Docker resource limits, workload characteristics, and concurrency.

---

# Architecture

The system is organized around a Fastify REST API, a log ingestion pipeline, PostgreSQL persistence, querying, and aggregation.

```mermaid
flowchart LR

    Client["Client"]
    Benchmark["Load Generator"]

    API["Fastify API"]

    Validation["Zod Validation"]

    Ingestion["Log Ingestion"]

    Batcher["Insert Batcher"]

    Query["Query Service"]

    Aggregation["Aggregation Service"]

    DB[("PostgreSQL")]

    Logs[("logs")]

    Rollups[("logs_rollup_1m")]

    Client --> API
    Benchmark --> API

    API --> Validation

    Validation --> Ingestion

    Ingestion --> Batcher

    Batcher --> DB

    API --> Query
    Query --> DB

    API --> Aggregation
    Aggregation --> DB

    DB --> Logs
    DB --> Rollups
```

---

# Log Ingestion Flow

The ingestion pipeline validates incoming logs, batches accepted entries, and writes them to PostgreSQL.

```mermaid
flowchart TD

    A["POST /logs"]

    B["Request Validation"]

    C{"Valid Entry?"}

    D["Reject Entry"]

    E["Accepted Logs"]

    F["Insert Batcher"]

    G["Batch Flush"]

    H["Database Transaction"]

    I[("PostgreSQL")]

    J[("logs")]

    K[("logs_rollup_1m")]

    A --> B

    B --> C

    C -->|No| D
    C -->|Yes| E

    E --> F
    F --> G
    G --> H

    H --> I

    I --> J
    I --> K
```

---

# Batch Processing

The service uses an internal batching mechanism to combine multiple incoming requests before writing to PostgreSQL.

This reduces database round trips and improves ingestion throughput.

The batching configuration uses:

```text
TARGET_FLUSH_SIZE = 4000
MAX_CONCURRENT_FLUSHES = 3
```

The basic processing model is:

```mermaid
flowchart LR

    Requests["Incoming Requests"]

    Queue["In-Memory Batch Queue"]

    Batch["Batch of Logs"]

    Flush["Concurrent Flush"]

    DB[("PostgreSQL")]

    Requests --> Queue
    Queue --> Batch
    Batch --> Flush
    Flush --> DB
```

Batching allows the service to process large numbers of log entries using fewer database operations.

---

# Database Design

PostgreSQL is used as the primary persistent storage engine.

## Entity Relationship Diagram

```mermaid
erDiagram

    LOGS {
        BIGSERIAL id PK
        TIMESTAMPTZ timestamp
        TEXT level
        TEXT service
        TEXT message
        JSONB attributes
    }

    LOGS_ROLLUP_1M {
        TIMESTAMPTZ bucket_start PK
        VARCHAR service PK
        VARCHAR level PK
        BIGINT count
    }

    LOGS ||--o{ LOGS_ROLLUP_1M : "aggregated into"
```

> `logs_rollup_1m` contains derived aggregation data. The relationship shown above represents a logical aggregation relationship rather than a direct foreign-key relationship.

---

# `logs` Table

The `logs` table stores the raw application log events.

| Column       | Type          | Description                    |
| ------------ | ------------- | ------------------------------ |
| `id`         | `BIGSERIAL`   | Unique log identifier          |
| `timestamp`  | `TIMESTAMPTZ` | Timestamp of the log           |
| `level`      | `TEXT`        | Log severity                   |
| `service`    | `TEXT`        | Service that generated the log |
| `message`    | `TEXT`        | Log message                    |
| `attributes` | `JSONB`       | Structured metadata            |

Supported levels:

```text
debug
info
warn
error
```

---

# `logs_rollup_1m` Table

The rollup table stores pre-aggregated statistics using one-minute time buckets.

| Column         | Type          | Description                 |
| -------------- | ------------- | --------------------------- |
| `bucket_start` | `TIMESTAMPTZ` | Start of aggregation bucket |
| `service`      | `VARCHAR`     | Service name                |
| `level`        | `VARCHAR`     | Log level                   |
| `count`        | `BIGINT`      | Number of logs              |

Primary key:

```sql
PRIMARY KEY (bucket_start, service, level)
```

The rollup table allows common aggregation queries to avoid repeatedly scanning the raw log dataset.

---

# Validation

Incoming logs are validated using **Zod** before being accepted by the ingestion pipeline.

A log entry contains:

```text
timestamp
level
service
message
attributes
```

The service validates:

* Required fields
* Log level
* Timestamp format
* Timestamp freshness
* Attribute types
* Request structure

Supported attribute value types include:

```text
string
number
boolean
```

The service also rejects timestamps that are more than five minutes in the future.

---

# Partial Batch Handling

The ingestion API supports partial success.

For example, if a request contains multiple logs and one entry is invalid:

```text
Valid log
Valid log
Invalid log
Valid log
```

The valid entries can still be processed while the invalid entry is reported to the client.

Conceptually:

```mermaid
flowchart LR

    Batch["Incoming Batch"]

    Valid["Valid Entries"]

    Invalid["Invalid Entries"]

    Database[("PostgreSQL")]

    Response["Response"]

    Batch --> Valid
    Batch --> Invalid

    Valid --> Database
    Invalid --> Response
    Database --> Response
```

This prevents a single malformed log entry from unnecessarily invalidating an entire batch.

---

# API

## `GET /health`

Checks application and PostgreSQL availability.

### Request

```http
GET /health
```

### cURL

```bash
curl http://localhost:8080/health
```

### Successful Response

```json
{
  "status": "ok"
}
```

---

# `POST /logs`

Ingests a batch of log entries.

### Request

```http
POST /logs
Content-Type: application/json
```

### Example

```json
{
  "logs": [
    {
      "timestamp": "2026-08-20T10:00:00.000Z",
      "level": "info",
      "service": "api",
      "message": "User logged in",
      "attributes": {
        "user_id": "123",
        "region": "eu-west"
      }
    }
  ]
}
```

### cURL

```bash
curl -X POST http://localhost:8080/logs \
  -H "Content-Type: application/json" \
  -d '{
    "logs": [
      {
        "timestamp": "2026-08-20T10:00:00.000Z",
        "level": "info",
        "service": "api",
        "message": "User logged in",
        "attributes": {
          "user_id": "123",
          "region": "eu-west"
        }
      }
    ]
  }'
```

### Response

```json
{
  "accepted": 1,
  "rejected": []
}
```

---

# `GET /logs`

Queries stored logs.

Supported filters include:

| Parameter | Description              |
| --------- | ------------------------ |
| `service` | Filter by service        |
| `level`   | Filter by log level      |
| `since`   | Lower timestamp boundary |
| `until`   | Upper timestamp boundary |
| `q`       | Search log messages      |
| `attr.*`  | Filter JSONB attributes  |
| `limit`   | Number of returned logs  |
| `cursor`  | Pagination cursor        |

Example:

```http
GET /logs?service=api&level=error&limit=100
```

---

# Attribute Filtering

Structured attributes can be queried using the `attr.` prefix.

Example:

```http
GET /logs?attr.region=eu-west
```

Another example:

```http
GET /logs?attr.user_id=123
```

This allows clients to filter logs without requiring every possible metadata field to become a dedicated database column.

---

# Message Search

The query API supports searching the `message` field.

Example:

```http
GET /logs?q=database
```

PostgreSQL trigram indexing is used to improve substring search performance.

The database enables the `pg_trgm` extension and creates a GIN index over the message column.

---

# Cursor-Based Pagination

The query API uses cursor-based pagination.

Results are ordered deterministically using:

```sql
ORDER BY timestamp DESC, id DESC
```

A cursor represents the last returned `(timestamp, id)` pair.

Conceptually:

```mermaid
flowchart LR

    Request["GET /logs"]

    Query["Query Database"]

    Page["Current Page"]

    Cursor["next_cursor"]

    Next["Next Request"]

    Request --> Query
    Query --> Page
    Page --> Cursor
    Cursor --> Next
    Next --> Query
```

Cursor pagination avoids the performance problems associated with large `OFFSET` values and provides stable traversal through the dataset.

---

# Aggregation

## `GET /logs/aggregate`

The aggregation endpoint provides time-based statistics.

Supported bucket sizes include:

```text
1m
5m
1h
1d
```

The aggregation can be grouped by dimensions such as:

```text
service
level
```

Example:

```http
GET /logs/aggregate?since=2026-08-20T00:00:00Z&until=2026-08-20T12:00:00Z&bucket=1h&group_by=service
```

---

# Aggregation Strategy

The service can use pre-aggregated rollups for common aggregation queries.

```mermaid
flowchart TD

    Request["Aggregation Request"]

    Filter{"Requires Raw Log Filtering?"}

    Rollup[("logs_rollup_1m")]

    Raw[("logs")]

    Aggregate["Time-Based Aggregation"]

    Result["Aggregation Result"]

    Request --> Filter

    Filter -->|No| Rollup
    Filter -->|Yes| Raw

    Rollup --> Aggregate
    Raw --> Aggregate

    Aggregate --> Result
```

### Rollup Path

When the aggregation does not require raw message or attribute filtering, the pre-aggregated rollup table can significantly reduce the amount of data that needs to be scanned.

### Raw Log Path

Queries requiring:

* Message search
* Attribute filtering

can operate on the raw `logs` table.

---

# Database Indexing

The database is optimized around common query patterns.

## Timestamp Index

Used for recent-log queries and cursor pagination.

```sql
(timestamp DESC, id DESC)
```

## Service Index

Used for service-filtered queries.

```sql
(service, timestamp DESC, id DESC)
```

## Level Index

Used for level-filtered queries.

```sql
(level, timestamp DESC, id DESC)
```

## JSONB Index

A GIN index is used to support structured attribute searches.

## Message Search Index

PostgreSQL `pg_trgm` is used for efficient substring searches.

```sql
CREATE INDEX idx_logs_message_trgm
ON logs USING GIN (message gin_trgm_ops);
```

---

# Transactional Writes

Raw logs and rollup updates are persisted within a database transaction.

```mermaid
sequenceDiagram

    participant Service
    participant PostgreSQL

    Service->>PostgreSQL: BEGIN

    Service->>PostgreSQL: Insert raw logs

    Service->>PostgreSQL: Upsert rollups

    PostgreSQL-->>Service: Success

    Service->>PostgreSQL: COMMIT
```

If an error occurs:

```mermaid
sequenceDiagram

    participant Service
    participant PostgreSQL

    Service->>PostgreSQL: BEGIN
    Service->>PostgreSQL: Insert logs
    Service->>PostgreSQL: Update rollups

    PostgreSQL-->>Service: Error

    Service->>PostgreSQL: ROLLBACK
```

This prevents raw logs and their corresponding rollups from becoming inconsistent.

---

# Deadlock Handling

Concurrent database writes can occasionally encounter transient PostgreSQL deadlocks.

The ingestion service includes retry handling for deadlock errors.

The current implementation retries failed transactions before returning an error to the caller.

This improves resilience when multiple ingestion batches are being flushed concurrently.

---

# Performance Engineering

The project applies several techniques to improve throughput.

### Batch Inserts

Multiple logs are inserted together rather than issuing one database operation per log.

### Concurrent Flushes

The ingestion pipeline allows multiple batches to be processed concurrently.

```text
Incoming Logs
      |
      v
Batch Queue
      |
      +----------+----------+
      |          |          |
      v          v          v
   Batch 1    Batch 2    Batch 3
      |          |          |
      +----------+----------+
                 |
                 v
             PostgreSQL
```

### Rollups

One-minute aggregation data reduces repeated work for common aggregation queries.

### PostgreSQL Indexing

Indexes are designed around actual query patterns rather than adding indexes indiscriminately.

### Cursor Pagination

Cursor pagination avoids increasingly expensive large-offset queries.

---

# Load Testing

The project is evaluated using a dedicated benchmark suite.

The benchmark measures:

| Metric            | Description                           |
| ----------------- | ------------------------------------- |
| Correctness       | Functional correctness checks         |
| Throughput        | Logs processed per second             |
| Error Rate        | Failed requests                       |
| p95               | Tail latency                          |
| Query Performance | Aggregation latency                   |
| Consistency       | Aggregation correctness               |
| Reliability       | Behavior across reliability scenarios |

---

# Benchmark Results

The current benchmark result is:

```text
Correctness    15.0 / 15
Performance    34.5 / 50
Queries        13.9 / 15
Reliability    20.0 / 20

Total          83.5 / 100
```

The service achieved:

```text
Throughput     14,636 logs/sec
Errors         0.0%
Ingestion p95  1,150 ms
Aggregate p95  59 ms
Consistency    4/4
Reliability    4/4
Correctness    15/15
```

This demonstrates a strong balance between throughput, correctness, query performance, and reliability.

---

# Docker

The repository includes Docker configuration for running the service together with PostgreSQL.

## Requirements

* Docker Desktop
* Docker Compose

## Start

```bash
docker compose up --build
```

## Check Containers

```bash
docker compose ps
```

## Application Logs

```bash
docker compose logs app
```

## PostgreSQL Logs

```bash
docker compose logs postgres
```

## Stop

```bash
docker compose down
```

To remove the database volume:

```bash
docker compose down -v
```

---

# Local Development

## Requirements

* Node.js 22+
* npm
* PostgreSQL

## Install Dependencies

```bash
npm install
```

## Environment Variables

Create a `.env` file:

```env
DATABASE_URL=postgresql://logs_user:logs_password@localhost:5432/logs_db
PORT=8080
HOST=0.0.0.0
```

## Start Development Server

```bash
npm run dev
```

## Type Check

```bash
npm run typecheck
```

## Build

```bash
npm run build
```

## Start Production Build

```bash
npm start
```

---

# Database Migrations

Database migrations are executed during application startup.

Migration files:

```text
src/db/migrations/
├── 001_initial.sql
├── 002_attr_kv_and_rollup.sql
└── 003_performance_tuning.sql
```

The migration system is responsible for creating and configuring the required PostgreSQL schema.

The migrations include:

* Raw logs table
* Rollup table
* Query indexes
* JSONB attribute support
* PostgreSQL trigram extension
* Message search index
* Performance-oriented indexes

---

# Project Structure

```text
log-ingestion-service-final/
│
├── src/
│   │
│   ├── db/
│   │   ├── migrations/
│   │   │   ├── 001_initial.sql
│   │   │   ├── 002_attr_kv_and_rollup.sql
│   │   │   └── 003_performance_tuning.sql
│   │   │
│   │   ├── migrate.ts
│   │   └── pool.ts
│   │
│   ├── routes/
│   │   ├── health.ts
│   │   └── logs.ts
│   │
│   ├── schemas/
│   │   └── logs.ts
│   │
│   ├── services/
│   │   └── log-service.ts
│   │
│   ├── app.ts
│   └── server.ts
│
├── Dockerfile
├── docker-compose.yml
├── benchmark-report.json
├── package.json
├── package-lock.json
├── tsconfig.json
└── README.md
```

---

# Technology Stack

| Technology        | Purpose                      |
| ----------------- | ---------------------------- |
| TypeScript        | Application development      |
| Node.js           | Runtime                      |
| Fastify           | HTTP server                  |
| PostgreSQL        | Persistent storage           |
| `pg`              | PostgreSQL client            |
| `pg-copy-streams` | PostgreSQL streaming support |
| Zod               | Request validation           |
| Docker            | Containerization             |
| Docker Compose    | Local orchestration          |
| K6                | Load testing                 |

---

# Engineering Decisions

## Fastify

Fastify provides a lightweight and performance-oriented HTTP framework for Node.js.

It is well suited to an ingestion service where request processing overhead matters.

## PostgreSQL

PostgreSQL was selected because it provides:

* Strong transactional guarantees
* JSONB support
* Advanced indexing
* Powerful aggregation
* Mature SQL capabilities
* Reliable persistence
* Extensions such as `pg_trgm`

## JSONB

Log metadata can vary between services.

JSONB allows the system to store dynamic attributes without changing the relational schema for every new metadata field.

Example:

```json
{
  "user_id": "123",
  "region": "eu-west",
  "request_id": "req-456"
}
```

## Batch Processing

Individual database operations for every log would introduce unnecessary overhead.

Batching allows many log records to be processed together, reducing database round trips.

## Cursor Pagination

Cursor pagination provides stable and efficient traversal of large datasets.

## Rollups

Pre-aggregated one-minute data reduces the cost of repeated aggregation queries.

## Transactions

Raw logs and rollups are committed together to maintain consistency.

---

# Reliability

The service includes several mechanisms designed to maintain correctness under load:

* Zod validation
* Partial-batch rejection
* Timestamp validation
* Transactional writes
* Deadlock retries
* Cursor validation
* Deterministic pagination
* PostgreSQL constraints
* Health checks
* Rollup consistency
* Docker health checks

The benchmark confirms:

```text
Correctness: 15/15
Consistency:  4/4
Reliability:  4/4
Error Rate:   0.0%
```

---

# Security Considerations

This project is primarily focused on backend engineering and performance.

Before production deployment, additional security controls should be considered:

* Authentication
* Authorization
* Rate limiting
* TLS
* Secret management
* Credential rotation
* Request-size limits
* Network isolation
* Monitoring
* Audit logging

Development database credentials should never be reused in production.

---

# Future Improvements

Potential improvements include:

* [ ] Horizontal application scaling
* [ ] Dedicated ingestion workers
* [ ] Message queue integration
* [ ] Redis caching
* [ ] Prometheus metrics
* [ ] OpenTelemetry tracing
* [ ] Database partitioning
* [ ] Automated CI/CD
* [ ] Benchmark regression testing
* [ ] Authentication and authorization
* [ ] Adaptive batching
* [ ] Improved observability
* [ ] Distributed ingestion

---

# What This Project Demonstrates

## Backend Engineering

* TypeScript
* Node.js
* Fastify
* REST API design
* Request validation

## Database Engineering

* PostgreSQL
* SQL
* JSONB
* GIN indexes
* Trigram search
* Transactions
* Aggregation
* Rollups
* Database migrations

## Performance Engineering

* Batch ingestion
* Bulk database writes
* Concurrent flushing
* Cursor pagination
* Query optimization
* Pre-aggregated data
* Load testing
* Stress testing

## Reliability Engineering

* Transactional writes
* Deadlock retries
* Input validation
* Partial batch handling
* Deterministic pagination
* Health checks
* Consistency validation

## DevOps

* Docker
* Docker Compose
* PostgreSQL health checks
* Resource management
* Reproducible development environment

---

# Author

## Jana Thuluth

GitHub:
https://github.com/JanaThuluth/log-ingestion-service-final

---

<p align="center">
  <strong>Designed for reliable, high-throughput log ingestion and querying.</strong>
</p>