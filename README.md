# Log Ingestion Service

<p align="center">
  <strong>High-performance log ingestion and query service built for reliable ingestion, efficient querying, and scalable aggregation.</strong>
</p>

<p align="center">
  Built with TypeScript, Fastify, PostgreSQL, Zod, and Docker.
</p>

<p align="center">

![TypeScript](https://img.shields.io/badge/TypeScript-5.x-3178C6?style=for-the-badge\&logo=typescript\&logoColor=white)
![Node.js](https://img.shields.io/badge/Node.js-22+-339933?style=for-the-badge\&logo=node.js\&logoColor=white)
![Fastify](https://img.shields.io/badge/Fastify-5.x-000000?style=for-the-badge\&logo=fastify\&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-18-4169E1?style=for-the-badge\&logo=postgresql\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge\&logo=docker\&logoColor=white)
![Zod](https://img.shields.io/badge/Zod-Validation-3E67B1?style=for-the-badge)

</p>

---

## Overview

**Log Ingestion Service** is a backend system designed to ingest, persist, query, and aggregate large volumes of application logs.

The service is designed around write-heavy workloads and focuses on:

* High-throughput batch ingestion
* Input validation
* Partial-batch error handling
* Efficient PostgreSQL writes
* Concurrent ingestion batching
* Cursor-based pagination
* Flexible JSONB attributes
* Full-text message search
* Time-based aggregation
* Pre-aggregated one-minute rollups
* Database indexing
* Transactional writes
* Deadlock retry handling
* Dockerized deployment
* Load and stress testing

The project uses **Fastify** for the HTTP layer, **PostgreSQL** for persistence, **Zod** for request validation, and PostgreSQL-specific optimizations for ingestion and querying.

---

## Architecture

```mermaid
flowchart LR

    Client["Client"]

    API["Fastify API"]

    Validation["Zod Validation"]

    Ingestion["Log Ingestion"]

    Batcher["Insert Batcher"]

    DB[("PostgreSQL")]

    Logs[("logs")]
    Rollups[("logs_rollup_1m")]

    Query["Query Service"]

    Aggregation["Aggregation Service"]

    Health["Health Check"]

    Client --> API

    API --> Validation

    Validation --> Ingestion

    Ingestion --> Batcher

    Batcher --> DB

    DB --> Logs
    DB --> Rollups

    API --> Query
    Query --> DB

    API --> Aggregation
    Aggregation --> DB

    API --> Health
    Health --> DB
```

---

## Ingestion Pipeline

The ingestion path validates incoming logs before they reach the database.

```mermaid
flowchart TD

    A["POST /logs"]

    B["Validate Request"]

    C{"Valid Log Entry?"}

    D["Add to Rejected List"]

    E{"Timestamp Valid?"}

    F["Reject Entry"]

    G["Accepted Logs"]

    H["InsertBatcher"]

    I["Batch Aggregation"]

    J["Transactional Database Write"]

    K[("PostgreSQL")]

    L[("logs")]

    M[("logs_rollup_1m")]

    A --> B
    B --> C

    C -->|No| D
    C -->|Yes| E

    E -->|No| F
    E -->|Yes| G

    G --> H
    H --> I
    I --> J
    J --> K

    K --> L
    K --> M
```

### Batch Processing

The ingestion service uses an internal batching queue to combine incoming requests before writing them to PostgreSQL.

The current implementation targets:

* **4000 logs per flush**
* **Up to 3 concurrent flushes**

This reduces the overhead of executing individual database writes and allows multiple incoming requests to be combined into larger database operations.

---

## Database Write Strategy

The service performs transactional writes for both raw logs and their corresponding rollup records.

```mermaid
sequenceDiagram
    participant Client
    participant API as Fastify API
    participant Batcher as Insert Batcher
    participant DB as PostgreSQL

    Client->>API: POST /logs
    API->>API: Validate entries
    API->>Batcher: Queue valid logs
    Batcher->>Batcher: Build batch
    Batcher->>DB: BEGIN
    Batcher->>DB: Bulk insert logs
    Batcher->>DB: Upsert 1-minute rollups
    DB-->>Batcher: Success
    Batcher->>DB: COMMIT
    Batcher-->>API: Inserted count
    API-->>Client: Accepted / Rejected
```

Raw log insertion and rollup updates are committed in the same transaction. If the transaction fails, the operation is rolled back.

The implementation also retries transient PostgreSQL deadlock errors up to two additional attempts before returning the failure.

---

# Data Model

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

    LOGS ||--o{ LOGS_ROLLUP_1M : "aggregates into"
```

> `logs_rollup_1m` contains derived aggregation data. The relationship shown above represents the logical aggregation between raw logs and rollup statistics rather than a foreign-key relationship.

---

## `logs`

The `logs` table stores the raw log events.

| Column       | Type          | Description           |
| ------------ | ------------- | --------------------- |
| `id`         | `BIGSERIAL`   | Unique log identifier |
| `timestamp`  | `TIMESTAMPTZ` | Log timestamp         |
| `level`      | `TEXT`        | Log severity          |
| `service`    | `TEXT`        | Service name          |
| `message`    | `TEXT`        | Log message           |
| `attributes` | `JSONB`       | Structured metadata   |

The schema uses indexes for timestamp ordering, service filtering, level filtering, and JSONB attribute access.

---

## `logs_rollup_1m`

The rollup table stores aggregated statistics in one-minute buckets.

| Column         | Type           | Description                 |
| -------------- | -------------- | --------------------------- |
| `bucket_start` | `TIMESTAMPTZ`  | Start of aggregation bucket |
| `service`      | `VARCHAR(255)` | Service name                |
| `level`        | `VARCHAR(10)`  | Log level                   |
| `count`        | `BIGINT`       | Number of logs              |

Primary key:

```sql
PRIMARY KEY (bucket_start, service, level)
```

This structure allows efficient aggregation queries without repeatedly scanning all raw logs for common aggregation workloads.

---

# Validation

Log entries are validated using Zod.

Each log must contain:

```text
timestamp
level
service
message
attributes
```

Supported log levels:

```text
debug
info
warn
error
```

Attributes support:

```text
string
number
boolean
```

The API also rejects timestamps that are more than five minutes in the future.

Invalid entries inside an otherwise valid batch are reported individually rather than causing valid entries to be discarded.

---

# API

## Health Check

### `GET /health`

Checks PostgreSQL connectivity.

```bash
curl http://localhost:8080/health
```

Successful response:

```json
{
  "status": "ok"
}
```

If the database is unavailable:

```json
{
  "status": "unavailable"
}
```

---

# Log Ingestion

## `POST /logs`

Accepts a batch of log entries.

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

For partially invalid batches:

```json
{
  "accepted": 2,
  "rejected": [
    {
      "index": 1,
      "reason": "invalid log entry"
    }
  ]
}
```

---

# Log Querying

## `GET /logs`

Returns stored logs.

Supported filters include:

| Parameter | Description             |
| --------- | ----------------------- |
| `service` | Filter by service       |
| `level`   | Filter by log level     |
| `since`   | Lower timestamp bound   |
| `until`   | Upper timestamp bound   |
| `q`       | Search message text     |
| `attr.*`  | Filter JSONB attributes |
| `limit`   | Number of results       |
| `cursor`  | Cursor for pagination   |

The `limit` value is restricted to:

```text
1 - 1000
```

### Example

```http
GET /logs?service=api&level=error&limit=100
```

### Attribute Filtering

Attributes can be queried using the `attr.` prefix.

Example:

```http
GET /logs?attr.region=eu-west
```

### Message Search

```http
GET /logs?q=database
```

The database uses PostgreSQL trigram indexing for efficient message search.

---

# Cursor-Based Pagination

The query endpoint uses cursor-based pagination rather than relying on large offsets.

Results are ordered by:

```sql
ORDER BY timestamp DESC, id DESC
```

A cursor contains:

```json
{
  "timestamp": "2026-08-20T10:00:00.000Z",
  "id": "12345"
}
```

The API returns:

```json
{
  "logs": [],
  "next_cursor": "..."
}
```

This provides deterministic ordering and avoids gaps or duplicates when traversing pages.

---

# Aggregation

## `GET /logs/aggregate`

Returns time-bucketed log statistics.

Required parameters:

```text
since
until
bucket
```

Supported buckets:

```text
1m
5m
1h
1d
```

Optional grouping:

```text
service
level
```

### Example

```http
GET /logs/aggregate?since=2026-08-20T00:00:00Z&until=2026-08-20T12:00:00Z&bucket=1h&group_by=service
```

### Example Response

```json
{
  "buckets": [
    {
      "start": "2026-08-20T00:00:00Z",
      "group": "api",
      "count": 12450
    }
  ]
}
```

---

# Aggregation Strategy

The service uses two aggregation paths.

```mermaid
flowchart TD

    Query["Aggregation Request"]

    Conditions{"Requires raw message or attribute filtering?"}

    Rollup[("logs_rollup_1m")]
    Raw[("logs")]

    Bucket["Time Bucketing"]
    Result["Aggregation Result"]

    Query --> Conditions

    Conditions -->|No| Rollup
    Conditions -->|Yes| Raw

    Rollup --> Bucket
    Raw --> Bucket

    Bucket --> Result
```

### Rollup Path

When the aggregation does not require raw message search or attribute filtering, the service can use `logs_rollup_1m`.

This reduces the amount of data that must be scanned.

### Raw Log Path

When the query requires:

* Message search
* Attribute filtering

the service queries the raw `logs` table.

---

# PostgreSQL Optimization

The database includes several indexes specifically designed around query patterns.

## Timestamp Index

```sql
(timestamp DESC, id DESC)
```

Used for deterministic ordering and cursor-based pagination.

## Service Index

```sql
(service, timestamp DESC, id DESC)
```

Optimizes service-filtered queries.

## Level Index

```sql
(level, timestamp DESC, id DESC)
```

Optimizes level-filtered queries.

## JSONB Attribute Index

The service uses a specialized PostgreSQL GIN index for normalized attribute key-value searches.

## Message Search Index

PostgreSQL `pg_trgm` is enabled for message search:

```sql
CREATE INDEX idx_logs_message_trgm
ON logs USING GIN (message gin_trgm_ops);
```

This supports more efficient substring matching for the `q` query parameter.

---

# Performance Engineering

The project includes several techniques intended to improve write throughput.

### Bulk Insert

Instead of inserting every log with an individual SQL statement, logs are converted into arrays and inserted using PostgreSQL `UNNEST`.

### Insert Batching

Multiple requests can be combined into larger database batches.

Current configuration:

```text
TARGET_FLUSH_SIZE = 4000
MAX_CONCURRENT_FLUSHES = 3
```

### Rollup Upserts

Rollup statistics are updated using:

```sql
ON CONFLICT (bucket_start, service, level)
DO UPDATE
```

This allows the service to increment existing aggregation buckets without rebuilding them from scratch.

### Transactional Writes

Raw logs and rollups are updated inside the same transaction.

---

# Benchmark Results

The repository includes a benchmark report generated by the project benchmark tool.

The recorded benchmark was executed using:

```text
Benchmark Tool: @foothill/logs-benchmark
Mode: Docker Compose
Generator: grafana/k6:0.54.0
Generator CPUs: 4
Generator Memory: 1 GB
Resource Limits: Enforced
```

The benchmark report records **15/15 correctness checks passed**, including ingestion, validation, querying, pagination, and aggregation.

## Correctness

| Category              |      Result |
| --------------------- | ----------: |
| Correctness Checks    | **15 / 15** |
| Correctness Rate      |    **100%** |
| Reliability Scenarios |   **4 / 4** |
| Error Rate            |      **0%** |

---

## Throughput

| Scenario   |         Throughput | Error Rate |       p95 |
| ---------- | -----------------: | ---------: | --------: |
| Load       |     4,548 logs/sec |         0% |  4,814 ms |
| Stress     |     5,304 logs/sec |         0% | 12,309 ms |
| Spike      | **6,012 logs/sec** |         0% | 20,074 ms |
| Breakpoint |     4,383 logs/sec |         0% | 35,418 ms |

The highest recorded throughput in the benchmark was approximately **6,012 logs/sec** during the spike scenario.

> These results are environment-specific. The benchmark report indicates that the load generator was itself constrained during the test, so the offered load should not be interpreted as a guaranteed service ceiling.

---

# Benchmark Score

The attached benchmark report records:

| Category    |     Score | Maximum |
| ----------- | --------: | ------: |
| Correctness |     15.00 |      15 |
| Performance |     21.06 |      50 |
| Queries     |      6.00 |      15 |
| Reliability |     20.00 |      20 |
| **Total**   | **62.06** | **100** |

The project passed all correctness and reliability checks in the recorded benchmark.

---

# Docker

The repository provides a Docker Compose configuration containing:

```text
Application
PostgreSQL
Persistent PostgreSQL volume
Health check
Resource limits
```

The application is configured to run on:

```text
http://localhost:8080
```

PostgreSQL is exposed on:

```text
localhost:5432
```

The application waits for PostgreSQL to become healthy before starting.

---

# Running with Docker

## Requirements

* Docker Desktop
* Docker Compose

## Start

```bash
docker compose up --build
```

## Check Status

```bash
docker compose ps
```

## View Application Logs

```bash
docker compose logs app
```

## View PostgreSQL Logs

```bash
docker compose logs postgres
```

## Stop

```bash
docker compose down
```

To remove the database volume as well:

```bash
docker compose down -v
```

---

# Running Locally

## Requirements

* Node.js
* npm
* PostgreSQL

## Install

```bash
npm install
```

## Environment Variables

```env
DATABASE_URL=postgresql://logs_user:logs_password@localhost:5432/logs_db
PORT=8080
HOST=0.0.0.0
```

## Development

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

## Production

```bash
npm start
```

The project defines development, build, production, and type-check scripts in `package.json`.

---

# Database Migrations

Database migrations are automatically executed during application startup.

Migration files:

```text
src/db/migrations/
├── 001_initial.sql
├── 002_attr_kv_and_rollup.sql
└── 003_performance_tuning.sql
```

The migrations create:

* `logs`
* `logs_rollup_1m`
* JSONB attribute indexing
* Rollup indexes
* PostgreSQL trigram extension
* Message search index
* Query-oriented indexes

---

# Project Structure

```text
log-ingestion-service-final/
│
├── src/
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

The structure separates HTTP routes, validation schemas, database access, migrations, and log-processing logic.

---

# Technology Stack

| Technology        | Purpose                      |
| ----------------- | ---------------------------- |
| TypeScript        | Application development      |
| Node.js           | Runtime                      |
| Fastify           | HTTP server and routing      |
| PostgreSQL        | Persistent storage           |
| `pg`              | PostgreSQL client            |
| `pg-copy-streams` | PostgreSQL streaming support |
| Zod               | Input validation             |
| Docker            | Containerization             |
| Docker Compose    | Local orchestration          |
| K6                | Load testing                 |

---

# Engineering Decisions

## Why Fastify?

Fastify provides a lightweight HTTP layer designed for high-performance Node.js applications.

The application creates a Fastify instance with logging enabled and registers dedicated health and log routes.

## Why PostgreSQL?

PostgreSQL provides:

* Transactional guarantees
* JSONB support
* Advanced indexing
* Aggregation capabilities
* Reliable persistence
* Native support for extensions such as `pg_trgm`

## Why JSONB?

Log attributes are dynamic by nature.

Different services can attach different metadata without requiring a new relational column for every attribute.

Example:

```json
{
  "request_id": "req-123",
  "user_id": "42",
  "region": "eu-west"
}
```

## Why Cursor Pagination?

Cursor pagination provides stable traversal through a changing log dataset.

The implementation uses:

```sql
ORDER BY timestamp DESC, id DESC
```

and uses the `(timestamp, id)` pair as the cursor boundary.

## Why Rollups?

Repeated aggregation over a large raw log table can become expensive.

The one-minute rollup table provides a smaller data source for common time-based aggregation queries.

## Why Batch Inserts?

Database round trips can become a bottleneck when processing thousands of log records.

The service combines multiple incoming requests into larger batches before writing them to PostgreSQL.

---

# Reliability

The implementation includes several mechanisms designed to preserve correctness under load:

* Input validation
* Partial-batch rejection
* Timestamp validation
* Transactional writes
* Deadlock retry
* Deterministic pagination
* Cursor validation
* Database health checks
* PostgreSQL constraints
* Rollup consistency
* Docker health checks

The benchmark confirms that all recorded correctness checks passed and all four reliability scenarios completed successfully.

---

# Security Considerations

This project is intended primarily as a backend engineering and performance project.

Before production deployment, consider adding:

* Authentication
* Authorization
* Rate limiting
* TLS
* Secret management
* Database credential rotation
* Request-size limits
* Network isolation
* Monitoring
* Audit logging

Development database credentials in Docker Compose should not be reused in production.

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
* [ ] Improved observability
* [ ] Adaptive batching based on system load

---

# What This Project Demonstrates

This project demonstrates practical backend engineering skills across several areas.

### Backend Engineering

* TypeScript
* Node.js
* Fastify
* REST API design
* Request validation

### Database Engineering

* PostgreSQL
* SQL
* JSONB
* GIN indexes
* Trigram search
* Transactions
* Aggregation
* Rollups
* Database migrations

### Performance Engineering

* Batch ingestion
* Bulk database writes
* Concurrent flushes
* Cursor pagination
* Query optimization
* Pre-aggregated data
* Load testing
* Stress testing

### Reliability Engineering

* Transactional writes
* Deadlock retry
* Input validation
* Deterministic pagination
* Health checks
* Consistency validation

### DevOps

* Docker
* Docker Compose
* Resource limits
* PostgreSQL health checks
* Reproducible local environments

---

# Author

## Jana Thuluth

GitHub: `JanaThuluth`

---

<p align="center">
  <strong>Designed for reliable, high-throughput log ingestion and querying.</strong>
</p>