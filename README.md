# Real-Time ML Feature Store with Online/Offline Consistency

Dual-path feature store: offline path using batch materialization into PostgreSQL with point-in-time correctness for training, and online path serving low-latency feature lookups from Redis for inference. Kafka-based change propagation keeps both stores eventually consistent, with a schema registry enforcing feature contracts across producer and consumer services.

## Stack

Python, Apache Kafka, Redis, PostgreSQL, FastAPI, Docker Compose

## Architecture

```
                  ┌────────────────────────────────────────────────────────┐
                  │                  Docker Compose (6 services)           │
                  │                                                        │
  POST /features/ │   FastAPI API                                          │
  write ─────────►│     │                                                  │
                  │     ├──► PostgreSQL (offline store)                    │
                  │     │     append-only feature_values table             │
                  │     │     with PIT indexes                             │
                  │     │                                                  │
                  │     └──► Kafka (feature-updates topic)                 │
                  │               │                                        │
                  │               ▼                                        │
                  │          Kafka Consumer (dual-write worker)            │
                  │               ├──► PostgreSQL (offline, at-least-once) │
                  │               └──► Redis (online, low-latency)         │
                  │                                                        │
  POST /features/ │                                                        │
  online ◄────────│   Redis ◄── hash per entity, TTL-managed               │
                  │                                                        │
  POST /features/ │                                                        │
  historical ◄────│   PostgreSQL ◄── LATERAL JOIN PIT query                │
                  │                                                        │
  POST /materialize│                                                       │
  /{view}/sync ──►│   Batch materialization: PG → Redis (with Redis lock)  │
                  │                                                        │
  POST /schemas  ─│─► Schema Registry (PG-backed, Redis-cached)            │
                  │     compatibility: BACKWARD / FORWARD / FULL / NONE    │
                  └────────────────────────────────────────────────────────┘
```

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **Append-only offline store** | Feature values are never overwritten — every update is a new row with a timestamp. Enables point-in-time queries for any historical moment |
| **LATERAL JOIN PIT query** | Single-round-trip SQL resolves all (entity, as_of_time) pairs. No N+1 queries. Prevents future data leakage into training sets |
| **Kafka change propagation** | Decouples write path from online store population. Consumer does dual-write to PG + Redis with manual offset commit (at-least-once) |
| **Redis HASH per entity** | O(1) lookup per entity with per-field access. TTL-managed expiry. Pipeline batching for bulk writes |
| **Schema registry with compatibility** | BACKWARD/FORWARD/FULL modes mirror Confluent conventions. Schema validated at produce time — incompatible data never reaches Kafka |
| **Distributed materialization lock** | Redis SETNX with TTL prevents concurrent batch materializations for the same feature view |

## Quick Start

```bash
# Start all services
docker compose up -d

# Wait ~30s for Kafka to be ready, then register a feature view
curl -X POST http://localhost:8080/schemas \
  -H "Content-Type: application/json" \
  -d '{
    "spec": {
      "name": "user_features",
      "entity_column": "user_id",
      "features": [
        {"name": "purchase_count", "dtype": "int64"},
        {"name": "avg_order_value", "dtype": "float64"},
        {"name": "is_premium", "dtype": "bool", "default": false}
      ]
    },
    "compatibility_mode": "BACKWARD"
  }'

# Write features (→ PostgreSQL offline + Kafka → consumer → Redis online)
curl -X POST http://localhost:8080/features/write \
  -H "Content-Type: application/json" \
  -d '{
    "feature_view": "user_features",
    "rows": [
      {"entity_id": "user_1", "features": {"purchase_count": 10, "avg_order_value": 45.5, "is_premium": true}},
      {"entity_id": "user_2", "features": {"purchase_count": 3, "avg_order_value": 22.0, "is_premium": false}}
    ]
  }'

# Online feature lookup (Redis, low-latency)
curl -X POST http://localhost:8080/features/online \
  -H "Content-Type: application/json" \
  -d '{"feature_view": "user_features", "entity_ids": ["user_1", "user_2"]}'

# Point-in-time historical retrieval (PostgreSQL, training-safe)
curl -X POST http://localhost:8080/features/historical \
  -H "Content-Type: application/json" \
  -d '{
    "feature_view": "user_features",
    "entity_timestamps": [["user_1", "2026-06-27T06:00:00Z"]]
  }'

# Batch materialize offline → online
curl -X POST http://localhost:8080/materialize/user_features/sync

# Check schema compatibility before evolving
curl -X POST http://localhost:8080/schemas/check-compatibility \
  -H "Content-Type: application/json" \
  -d '{
    "feature_view": "user_features",
    "new_spec": {
      "name": "user_features",
      "entity_column": "user_id",
      "features": [
        {"name": "purchase_count", "dtype": "int64"},
        {"name": "avg_order_value", "dtype": "float64"},
        {"name": "is_premium", "dtype": "bool", "default": false},
        {"name": "loyalty_score", "dtype": "float64", "default": 0.0}
      ]
    }
  }'

# Health check
curl http://localhost:8080/health
```

## Services

| Service | Port | Description |
|---|---|---|
| **FastAPI API** | 8080 | Feature read/write, schema registry, materialization |
| **Kafka Consumer** | — | Change propagation worker (Kafka → PG + Redis dual-write) |
| **PostgreSQL** | 5433 | Offline feature store + schema registry + feature view catalog |
| **Redis** | 6380 | Online feature store (low-latency serving) |
| **Kafka** | 9093 | Change propagation (feature-updates, schema-changes topics) |
| **Zookeeper** | — | Kafka coordination |

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/schemas` | Register a feature view schema (with compatibility check) |
| `GET` | `/schemas/{view}` | Get latest schema for a feature view |
| `GET` | `/schemas/{view}/{version}` | Get specific schema version |
| `GET` | `/schemas/{view}/versions/all` | List all schema versions |
| `POST` | `/schemas/check-compatibility` | Dry-run compatibility check |
| `GET` | `/feature-views` | List all registered feature views |
| `GET` | `/feature-views/{name}` | Get feature view details |
| `DELETE` | `/feature-views/{name}` | Deactivate a feature view |
| `POST` | `/features/write` | Write feature rows (offline + Kafka propagation) |
| `POST` | `/features/online` | Low-latency feature lookup from Redis |
| `POST` | `/features/historical` | Point-in-time correct historical retrieval |
| `GET` | `/features/stats/{view}` | Offline/online store statistics |
| `POST` | `/materialize/{view}` | Trigger async batch materialization |
| `POST` | `/materialize/{view}/sync` | Trigger sync batch materialization |
| `GET` | `/materialize/jobs` | List materialization jobs |
| `GET` | `/health` | Health check (PG, Redis, Kafka) |

## Feature Types

Supported dtypes: `float64`, `int64`, `string`, `bool`, `list_float64`, `list_int64`, `bytes`

```json
{
  "name": "user_features",
  "entity_column": "user_id",
  "features": [
    {"name": "purchase_count", "dtype": "int64", "description": "Total purchases"},
    {"name": "embedding", "dtype": "list_float64", "description": "User embedding vector"},
    {"name": "is_premium", "dtype": "bool", "default": false}
  ]
}
```

## Schema Compatibility Modes

| Mode | Add optional field | Add required field | Remove field | Change type |
|---|---|---|---|---|
| **BACKWARD** | OK | OK | Blocked (no default) / Warn (has default) | Blocked |
| **FORWARD** | OK (with default) | Blocked | OK | Blocked |
| **FULL** | OK (with default) | Blocked | Blocked (no default) | Blocked |
| **NONE** | OK | OK | OK | OK |

## Running Tests

```bash
# Unit tests (schema compatibility — no external services)
pytest tests/test_schema_registry.py -v

# Integration tests (requires running Docker Compose stack)
pytest tests/test_online_store.py tests/test_point_in_time.py -v

# All tests
pytest tests/ -v
```

## Project Structure

```
├── docker-compose.yml          # 6-service orchestration
├── Dockerfile                  # API + consumer image
├── requirements.txt            # Python dependencies
├── .env / .env.example         # Environment configuration
├── src/
│   ├── main.py                 # FastAPI app + CLI (--mode api | consumer)
│   ├── config.py               # Pydantic settings
│   ├── cli.py                  # Rich CLI client
│   ├── api/
│   │   ├── features.py         # Feature write/read/historical endpoints
│   │   ├── schemas.py          # Schema registry + feature view endpoints
│   │   └── materialization.py  # Batch materialization endpoints
│   ├── db/
│   │   ├── postgres.py         # asyncpg pool + DDL migrations
│   │   └── redis_client.py     # Redis connection + key namespace helpers
│   ├── kafka/
│   │   ├── producer.py         # Schema-validated Kafka producer
│   │   ├── consumer.py         # Dual-write consumer (PG + Redis)
│   │   └── messages.py         # Kafka message serialization
│   ├── models/
│   │   └── schemas.py          # Pydantic models (features, schemas, jobs)
│   ├── store/
│   │   ├── feature_registry.py # Feature view catalog (PG + Redis cache)
│   │   ├── online_store.py     # Redis-backed online store
│   │   └── offline_store.py    # PostgreSQL-backed offline store with PIT
│   ├── materialization/
│   │   └── batch.py            # Batch offline → online with Redis lock
│   └── schema_registry/
│       ├── registry.py         # Versioned schema storage
│       └── compatibility.py    # BACKWARD/FORWARD/FULL/NONE checker
└── tests/
    ├── test_schema_registry.py # Schema compatibility unit tests (12 tests)
    ├── test_online_store.py    # Redis online store tests (7 tests)
    └── test_point_in_time.py   # PIT correctness tests (5 tests)
```
