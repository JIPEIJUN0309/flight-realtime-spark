# Real-Time Flight Data Platform

> A streaming + batch lakehouse pipeline built on **Kafka + Spark + Delta Lake**, ingesting live flight telemetry from the OpenSky Network and serving it to a real-time dashboard — with an offline batch path orchestrated by Airflow.

![Spark](https://img.shields.io/badge/Spark-Structured%20Streaming-E25A1C?logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)
![Delta](https://img.shields.io/badge/Delta%20Lake-Lakehouse-00ADD4)
![Airflow](https://img.shields.io/badge/Airflow-Orchestration-017CEE?logo=apacheairflow&logoColor=white)
![Postgres](https://img.shields.io/badge/PostgreSQL-Serving-4169E1?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)

---

## Table of Contents

- [What This Project Does](#what-this-project-does)
- [Tech Stack](#tech-stack)
- [Data Source](#data-source)
- [Pipeline Layers](#pipeline-layers)
- [Key Results](#key-results)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Technical Highlights](#technical-highlights)
- [Engineering Challenges Solved](#engineering-challenges-solved)

---

## What This Project Does

This project pulls **live aircraft state vectors** from the OpenSky Network, buffers them through Kafka, and refines them through a Delta Lake medallion architecture into metrics that drive a real-time dashboard.

```
OpenSky API  →  Kafka  →  Spark Structured Streaming  →  Delta Lake (bronze → silver → gold)
                                                              ↓
                                            PostgreSQL  →  Metabase dashboard
```

Two paths run over the same lakehouse tables:

- **Streaming path** — Structured Streaming consumes Kafka continuously, landing raw records in the bronze layer and maintaining windowed aggregates with watermarking.
- **Batch path** — Airflow orchestrates a scheduled Spark job over the same Delta tables, producing the serving layer idempotently.

Running both against identical logic makes the trade-offs between streaming and batch measurable rather than theoretical.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Ingestion** | Python producer · OpenSky REST API (OAuth2) |
| **Message queue** | Kafka (KRaft mode, no ZooKeeper) |
| **Compute** | Spark Structured Streaming · Spark SQL |
| **Lakehouse** | Delta Lake (bronze / silver / gold) |
| **Orchestration** | Airflow |
| **Serving** | PostgreSQL · Metabase |
| **Environment** | Docker Compose |

---

## Data Source

[OpenSky Network](https://opensky-network.org/) — a community-run receiver network publishing live ADS-B aircraft state vectors.

| Field | Description |
|-------|-------------|
| `icao24` | Unique 24-bit aircraft transponder address |
| `callsign` | Flight callsign |
| `latitude` / `longitude` | Current position |
| `baro_altitude` | Barometric altitude (m) |
| `velocity` | Ground speed (m/s) |
| `on_ground` | Whether the aircraft is on the ground |
| `snapshot_time` | Poll timestamp |

The API enforces a **4,000 requests/day** free-tier quota. A bounding box over European airspace (the densest traffic region) plus a tuned poll interval keeps consumption at roughly **2,880 requests/day** — comfortably inside the limit while maintaining ~30-second freshness.

---

## Pipeline Layers

| Layer | Role | Key Processing |
|-------|------|----------------|
| **Bronze** | Raw landing | Structured Streaming writes Kafka payloads to Delta as-is |
| **Silver** | Cleaned detail | Drop on-ground records, null coordinates, implausible altitude/velocity; derive typed columns |
| **Gold** | Aggregates | 5-minute tumbling windows with watermarking; traffic density and anomaly signals |
| **Serving** | Application | Delta `MERGE` into PostgreSQL, consumed by Metabase |

### Nearest-Airport Enrichment

The silver layer joins each aircraft against an airport dimension table using a **non-equi join**: the Haversine great-circle distance is computed against candidate airports, then `ROW_NUMBER()` picks the closest one per aircraft. This yields both the nearest airport and its distance without a pre-built spatial index.

---

## Key Results

### 1. Streaming vs. Batch, Same Logic

Both paths compute identical 5-minute window aggregates over the same Delta tables, which exposes where the two models genuinely differ.

The streaming version uses `approx_count_distinct` rather than exact distinct counts: on an unbounded stream, exact distinct state grows without limit, so a mergeable HyperLogLog sketch trades a small error for bounded memory. The batch version, operating on a finite partition, computes exact counts.

### 2. Engine Comparison — Spark SQL vs. Hive on Tez

Same aggregation SQL, same Parquet input, same machine:

| Dataset | Speedup |
|---------|---------|
| 136K rows | **≈3.9x** |
| 4.1M rows | **≈3.5x** |

Outputs were cross-validated between the two engines and matched exactly.

### 3. Spark Tuning — Measured, Not Assumed

Identical 4.1M-row workload, configuration changes only:

| Change | Speedup |
|--------|---------|
| Shuffle partitions 200 → 8 | **2.9x** |
| AQE auto-coalescing enabled | **2.6x** |
| Broadcast join vs. sort-merge (large table ⋈ small dimension) | **1.9x** |
| Partition pruning | `PartitionFilters` confirmed in the physical plan |

The default 200 shuffle partitions badly over-partition a dataset this size — each task ends up doing almost nothing while scheduling overhead dominates. AQE reaches a similar outcome automatically, which is why it is preferable to hand-tuning in production.

### 4. Orchestrated Batch Path

An Airflow DAG (`preflight → dwd_silver → dws_gold_batch → ads_export`) replaces three manually-issued Spark commands, running every 15 minutes.

| Metric | Value |
|--------|-------|
| Runs verified (manual + scheduled) | 2 / 2 successful |
| Average runtime | ~2 min 37 s |

`max_active_runs=1` prevents overlapping runs from writing over each other, and retries are configured per task — the pipeline is safe to re-run at any point.

---

## Quick Start

### Prerequisites

- Docker Desktop
- Python 3.12+
- An OpenSky account with an **API client** (`client_id` / `client_secret`) created under [Account settings](https://opensky-network.org/my-opensky/account)

> OpenSky retired username/password authentication in March 2026; the API now accepts only OAuth2 client-credentials.

### Run

```bash
# 1. Configure credentials
cp .env.example .env          # then fill in client_id / client_secret

# 2. Start Kafka (KRaft mode)
docker compose up -d
docker compose ps             # wait until kafka / kafka-ui are healthy

# 3. Start the producer
pip install -r requirements.txt
python opensky_producer.py

# 4. Start the Spark streaming jobs
#    (bronze ingest, silver cleanse, gold aggregation)

# 5. Optional: bring up the batch orchestration stack
docker compose -f docker-compose.airflow.yml up -d
```

Web UIs: Kafka UI at `http://localhost:8080` · Airflow at `http://localhost:8081`

---

## Project Structure

```
flight-realtime-spark/
├── README.md
├── docker-compose.yml            # Kafka (KRaft) + Kafka UI
├── docker-compose.hive.yml       # Hive on Tez, for the engine comparison
├── docker-compose.airflow.yml    # Airflow scheduler + webserver
├── .env.example                  # Credential template
├── requirements.txt
├── opensky_producer.py           # Polls OpenSky, publishes to Kafka
├── token_manager.py              # OAuth2 token acquisition and refresh
├── spark/                        # Streaming and batch Spark jobs
├── dim/                          # Airport dimension table
├── airflow/                      # DAG definitions
├── hive/                         # Hive DDL for the comparison workload
└── hive_vs_spark/                # Engine benchmark harness
```

---

## Technical Highlights

| Technique | What it does | Where |
|-----------|--------------|-------|
| **OAuth2 token refresh** | Acquires and transparently renews bearer tokens before expiry | Producer |
| **Bounding-box quota control** | Restricts the query region to stay inside the free-tier daily quota | Producer |
| **Kafka KRaft mode** | Runs Kafka without ZooKeeper — fewer moving parts | Ingestion |
| **Watermarking** | Bounds late-arriving event state in windowed aggregations | Gold |
| **HLL approximation** | `approx_count_distinct` keeps streaming distinct-count state bounded | Gold (streaming) |
| **Non-equi join** | Haversine distance + `ROW_NUMBER()` resolves nearest airport | Silver |
| **Delta MERGE** | Idempotent upsert into the serving table — safe to re-run | Serving |
| **`max_active_runs=1`** | Prevents concurrent DAG runs from overwriting each other | Orchestration |

---

## Engineering Challenges Solved

Real problems encountered and fixed during development.

1. **Authentication broke mid-project.**
   OpenSky retired username/password authentication in March 2026 in favour of OAuth2 client-credentials. Reworked the producer around a token manager that fetches a bearer token, tracks its ~1800-second lifetime, and refreshes ahead of expiry so long-running polls never fail on a stale token.

2. **A daily quota that a naive poller burns through by noon.**
   The free tier allows 4,000 requests/day. Polling globally at a short interval exhausts it quickly. Constraining the query to a European bounding box and tuning the poll interval brought steady-state usage to ~2,880 requests/day while keeping data roughly 30 seconds fresh.
   *Lesson: rate limits are a design input, not an afterthought.*

3. **Unbounded state in streaming distinct counts.**
   Exact `count(distinct)` over an unbounded stream accumulates state indefinitely. Switched the streaming path to `approx_count_distinct`, whose HyperLogLog sketches are mergeable and fixed-size, while the batch path retains exact counts.
   *Lesson: streaming and batch can share logic, but not always the same aggregate functions.*

4. **Default shuffle partitions dominated runtime.**
   Spark's default of 200 shuffle partitions over-partitions a 4.1M-row workload — per-task scheduling overhead outweighed the actual work. Hand-tuning to 8 gave a 2.9x speedup; enabling AQE reached 2.6x automatically, which is the more maintainable choice as data volume changes.

5. **Overlapping scheduled runs corrupted the serving table.**
   A 15-minute schedule with occasionally longer runtimes allowed two DAG runs to overlap and write over each other. Fixed with `max_active_runs=1` plus idempotent Delta `MERGE` writes, so any run can safely be retried or backfilled.

6. **Python 3.12 incompatibility in the Kafka client.**
   The long-standing `kafka-python` package does not support Python 3.12+. Switched to the maintained `kafka-python-ng` fork.

---

## License

For learning and portfolio purposes only. Flight data © OpenSky Network.
