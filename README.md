# Real-Time Flight Data Platform

> A streaming + batch lakehouse pipeline built on **Kafka + Spark + Delta Lake**: live flight telemetry from the OpenSky Network is ingested in real time, refined in Delta Lake, and served to a dashboard that an Airflow batch path refreshes every 15 minutes.

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
- [Anomaly Detection](#anomaly-detection)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Technical Highlights](#technical-highlights)
- [Engineering Challenges Solved](#engineering-challenges-solved)
- [Known Limitations](#known-limitations)

---

## What This Project Does

This project pulls **live aircraft state vectors** from the OpenSky Network, buffers them through Kafka, and refines them through a Delta Lake medallion architecture into a real-time operations dashboard and a 15-minute statistics report.

```
OpenSky API  →  Kafka  →  Spark Structured Streaming  →  Delta Lake (bronze → DWD → DWS → ADS)
                                                              ↓
                                    PostgreSQL  →  Grafana (5 s) / Metabase (15 min)
```

Two paths share the same Kafka topic and lakehouse:

- **Streaming path** — one Structured Streaming query consumes Kafka every 5 seconds and writes, in the same micro-batch, the raw ODS layer, the cleaned state detail (including ground records), take-off/landing events, flight legs and the latest position per aircraft. The same micro-batch upserts three of those tables into PostgreSQL in a single transaction, which is what the Grafana cards (5-second refresh) read.
- **Batch path** — every 15 minutes Airflow runs the DWS → DQ → ADS jobs over the Delta tables, each with an explicit time interval so any historical window can be recomputed independently. ADS tables are merged with interval-scoped deletes and copied to PostgreSQL for the statistics dashboards.

Running both makes the trade-offs between streaming and batch — exact versus approximate distinct counts, latency versus completeness, rule-based versus learned anomaly detection — measurable rather than theoretical.

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
| **ODS (bronze)** | Raw landing | Structured Streaming writes Kafka payloads to Delta as-is, plus Kafka offset and ingest time |
| **DWD** | State detail | Validity rules, dedupe by `(icao24, event_time)`, typed columns, nearest airport. Ground records are kept — take-off/landing detection depends on them |
| **DWD (derived)** | Events and legs | `on_ground` flips become take-off/landing events; airborne records are stitched into flight legs with a 20-minute timeout and a lost-reason classification |
| **DWS** | Aggregates | 5-minute airport windows with the approach criterion (≤ 30 km, ≤ 3,000 m, descending), hourly movements from the event table, country share, area overview. Recomputable per interval |
| **ADS / Serving** | Application | Interval-scoped Delta `MERGE` into ADS tables, then a JDBC copy into PostgreSQL, consumed by Grafana and Metabase |

### Nearest-Airport Enrichment

Each aircraft position is matched against a 20-row airport dimension table with no equality key — Spark plans it as a broadcast nested loop join. The Haversine great-circle distance is computed for every candidate airport and `ROW_NUMBER()` partitioned by `(icao24, event_time)` keeps the closest one. This yields both the nearest airport and its distance without a spatial index, and leaves exactly one row per `(icao24, event_time)`.

The implementation goes one step further: the 20-row airport list is broadcast as an array constant and the distance is computed row-locally with higher-order functions, so the upstream data never has to be shuffled or expanded to 20× its size before deduplication.

---

## Key Results

### 1. Real-Time Path — Measured, Not Assumed

Two hours of continuous operation at a 10-second poll interval, with a sampler writing one observation every 10 seconds and an atomicity probe every 0.2 seconds (720 samples, 722 data-bearing micro-batches, 34,191 probes):

| Metric | Measured | Target |
|--------|----------|--------|
| End-to-end freshness | P50 **14.0 s**, P95 **22.9 s** | P95 ≤ 30 s |
| Processing latency (Kafka append → serving layer) | P50 **7.0 s**, P95 **9.4 s** | P95 ≤ 10 s |
| Per-batch processing time | P50 **5.6 s**, P95 **6.2 s** | < 5 s (trigger interval) |
| Serving-layer atomicity | 0 half-batches, 0 empty tables | 0 |

The two customer-visible targets hold. The batch-time target does not hold during the day: traffic rose from 160 aircraft per snapshot at night to 636 on average and 1,328 at peak, and per-batch time scales with the number of records because flight-leg stitching re-reads a 20-minute lookback and runs window functions over it. On the same code at night (160–375 records per batch) the measurement was P50 4.6 s / P95 5.5 s.

Two conclusions, both actionable. The freshness and latency targets are met with margin at any traffic level measured. The throughput margin is limited by the leg step (about 4.3 s of a 5.6 s batch); the obvious next step is to stop re-reading the lookback from Delta every batch and broadcast only the aircraft in the current batch into the window functions.

### 2. Streaming vs. Batch

Both paths compute 5-minute window aggregates of the same metrics, which exposes where the two models genuinely differ.

Structured Streaming does not support distinct aggregations — `countDistinct` fails at analysis time — so the streaming version uses `approx_count_distinct` (HyperLogLog, about 5% relative error by default). Window state is bounded by the watermark either way. The batch version computes exact `COUNT(DISTINCT)`, so the two can be compared directly.

### 3. Engine Comparison — Spark SQL vs. Hive on Tez

Same aggregation SQL, same Parquet input, same machine:

| Dataset | Speedup |
|---------|---------|
| 136K rows | **≈3.9x** |
| 4.1M rows | **≈3.5x** |

Outputs were cross-validated between the two engines and matched exactly.

### 4. Spark Tuning — Measured, Not Assumed

Identical 4.1M-row workload (the collected data scaled 30x), configuration changes only:

| Change | Speedup |
|--------|---------|
| Shuffle partitions 200 → 8 | **2.9x** |
| AQE auto-coalescing enabled | **2.6x** |
| Broadcast join vs. sort-merge (large table ⋈ small dimension) | **1.9x** |
| Partition pruning | `PartitionFilters` confirmed in the physical plan |

The default 200 shuffle partitions badly over-partition a dataset this size — each task ends up doing almost nothing while scheduling overhead dominates. AQE reaches a similar outcome automatically, which is why it is preferable to hand-tuning in production.

### 5. Orchestrated Batch Path

An Airflow DAG replaces a sequence of manually-issued Spark commands, running every 15 minutes:

```
preflight → dwd_silver → dws_gold_batch → ads_export → ml_features → ml_score
```

| Metric | Value |
|--------|-------|
| Runs verified (manual + scheduled) | 2 / 2 successful |
| Average runtime | ~2 min 37 s |

*Timings were measured on the four-task chain, before the two anomaly-detection tasks were added.*

`max_active_runs=1` prevents overlapping runs from writing over each other, and retries are configured per task — the pipeline is safe to re-run at any point.

---

## Anomaly Detection

The serving layer already flagged anomalies with fixed thresholds. That rule path had a specific failure mode, and it is what motivated the model.

The first threshold for "rapid climb" was 15 m/s. Over 32,581 records it raised 623 alerts — a 1.9% alert rate. But 15 m/s is 2,953 ft/min, a perfectly normal initial climb for an airliner: the rule was alerting on takeoff. Recalibrating to 25 m/s using domain knowledge dropped it to a handful.

That fixed the constant, not the method. Fixed thresholds have three limits:

- **They judge one row at a time.** A single 26 m/s sample can be normal; three minutes of oscillating climb rate is not. A row-level predicate cannot tell those apart.
- **Every anomaly shape needs its own rule.** Altitude jumps, speed steps, position drift and reporting gaps are four separate predicates, and the list is not known in advance.
- **One constant cannot fit every aircraft and flight phase.** Normal climb rate differs by an order of magnitude between them.

So this module stops encoding *what counts as abnormal* and instead learns *what normal looks like*.

### Approach

An **LSTM autoencoder** over sequences of 20 consecutive samples — 10 minutes at a 30-second poll interval — across eight per-sample features: altitude, ground speed, vertical rate, acceleration, rate of change of vertical rate, turn rate, the residual between GPS-derived and reported speed, and the sampling-gap ratio.

The model is trained only to reconstruct its input. Reconstruction error becomes the anomaly score: sequences that do not lie on the manifold of normal flight are the ones it cannot reproduce.

An autoencoder rather than a classifier because **there are no labels**. No public ADS-B dataset carries anomaly annotations, and the same holds for most industrial telemetry — normal data is plentiful, real faults are rare and unlabelled. Self-supervised reconstruction needs only the assumption that most data is normal.

**The latent dimension is chosen by sweep, not assumption.** This is the method's principal failure mode: too wide and the autoencoder approaches an identity map, reconstructing anomalies as faithfully as normal data; too narrow and it reconstructs nothing. Note that lower validation reconstruction loss is *not* the selection criterion — low loss is often precisely the symptom of an over-wide bottleneck. The sweep selects on separation instead.

Splits are made **by aircraft, not by window**. With a stride of 5 over a window of 20, adjacent windows overlap 75%; a random window-level split puts near-identical sequences on both sides and inflates validation scores.

### Evaluation

`spark/ml_eval.py` compares three methods on the same held-out set:

| Method | Models the sequence? | Needs labels? |
|--------|----------------------|---------------|
| Fixed thresholds (the existing rule) | No | No |
| Isolation Forest | No — window aggregates only | No |
| LSTM autoencoder | Yes | No |

Isolation Forest is the informative baseline: it sees mean, standard deviation, min and max per feature, which discards ordering entirely — "rises then falls" and "falls then rises" collapse to the same vector. The gap between it and the autoencoder is the measurable value of modelling the sequence.

Scoring uses **PR-AUC rather than ROC-AUC**. Anomaly detection is severely imbalanced, and ROC's false-positive rate is diluted by an enormous negative class: a large absolute number of false alerts still reads as a small FPR.

Labels come from two directions, because neither is sufficient alone:

- **Synthetic injection** measures recall. Four anomaly shapes — altitude step, speed drop, position drift, reporting gap — are injected into held-out normal sequences at the *raw column* level, so the effect propagates through feature derivation rather than being written directly into the derived features.
- **A real false-positive regression set** measures over-firing. Windows whose peak vertical rate falls between the old 15 m/s threshold and the corrected 25 m/s one are precisely the alerts the first rule raised and domain knowledge later cleared as normal climbs — a set of human-confirmed negatives. A model that ranks them highly has only relearned the bad rule. Synthetic injection cannot test for this.

Alerting takes a quantile of the reconstruction-error distribution rather than an absolute cutoff — the original 1.9% alert rate came from guessing a constant — and requires N consecutive windows above threshold before firing, the standard debounce against single-sample spikes. Reconstruction error is also decomposed per feature, so an alert carries which signal drove it rather than only a score.

`ml_eval.py` prints the full three-way comparison, the recall/false-positive trade-off across thresholds and debounce settings, and the per-feature attribution.

Running instructions, the implementation decisions behind the module, and its known limitations are in [`docs/anomaly-detection.md`](docs/anomaly-detection.md).

### Pipeline Integration

Inference runs in the same batch chain and reuses the serving layer's `MERGE` helper with delete-on-missing semantics, since alerts are recomputed from the full source — changing the model or the threshold has to retire the previous run's rows rather than leave them behind. Results land in `ads_ml_anomaly` next to the existing rule alerts on the same time axis, so the two judgements can be compared directly in the dashboard.

**Training is deliberately not scheduled.** Inference reruns on every batch; training is an occasional model-iteration step that needs a human to read the sweep and decide whether to promote a new model. Scheduling it every 15 minutes would silently swap the production model several times an hour and make any regression impossible to trace.

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
cp .env.example .env          # then fill in client_id / client_secret and DATA_DIR (where the lake lives)

# 2. Start the main stack: Kafka (KRaft mode), Kafka UI, Spark, PostgreSQL, Metabase, Grafana
docker compose up -d
docker compose ps             # wait until kafka is healthy

# 3. Start the producer
pip install -r requirements.txt
python ingestion/opensky_producer.py

# 4. Start the real-time job: one micro-batch every 5 s lands bronze, cleans states, derives
#    airport events, flight legs and latest positions, and syncs PostgreSQL for the Grafana cards
docker exec -it spark /opt/spark/bin/spark-submit --driver-memory 2g /opt/spark/work-dir/jobs/rt_pipeline.py

# 5. Batch path: DWS -> DQ -> ADS (by hand, or let Airflow run it every 15 minutes)
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dws_batch.py
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dq_checks.py \
    --start "2026-10-20 00:00" --end "2026-10-20 06:00"
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/ads_export.py

# 6. Alert engine: polls PostgreSQL every 15 s, writes P1/P2 alerts and pushes to a webhook
docker exec -d spark python3 /opt/spark/work-dir/jobs/alert_engine.py

# 7. Optional: bring up the Airflow stack
docker compose -f docker-compose.airflow.yml up -d
```

Web UIs: Kafka UI `http://localhost:8090` · Spark UI `http://localhost:4040` · Metabase `http://localhost:3001` · Grafana `http://localhost:3002` · Airflow `http://localhost:8081`

---

## Project Structure

```
flight-realtime-spark/
├── README.md
├── docker-compose.yml            # Kafka (KRaft), Kafka UI, Spark, PostgreSQL, Metabase, Grafana
├── docker-compose.hive.yml       # Hive on Tez, for the engine comparison
├── docker-compose.airflow.yml    # Airflow scheduler + webserver
├── .env.example                  # Credential template
├── requirements.txt
├── ingestion/                    # OpenSky poller (Kafka producer) and OAuth2 token manager
├── spark/                        # Streaming job, batch jobs, rules, DQ checks, alert engine
├── airflow/                      # DAG definitions
├── grafana/                      # Real-time cards (provisioned datasource + dashboard)
├── sql/                          # Data-quality and reconciliation SQL
├── dim/                          # Airport dimension table
├── tests/                        # Unit and data-quality tests
├── scripts/                      # Operational scripts (topic setup, backfill, replay)
├── docs/                         # Requirements, architecture, runbook, decision records
├── hive/                         # Hive image (PostgreSQL JDBC driver) for the engine comparison
├── hive_vs_spark/                # Engine benchmark harness
└── .github/workflows/            # CI: syntax check + unit tests
```

Design documents live in [`docs/`](docs/): requirements and metric definitions ([`requirements.md`](docs/requirements.md), in Chinese), architecture, runbook and decision records.

---

## Technical Highlights

| Technique | What it does | Where |
|-----------|--------------|-------|
| **OAuth2 token refresh** | Acquires and transparently renews bearer tokens before expiry | Producer |
| **Bounding-box quota control** | Restricts the query region to stay inside the free-tier daily quota | Producer |
| **Kafka KRaft mode** | Runs Kafka without ZooKeeper — fewer moving parts | Ingestion |
| **Watermarking** | Bounds late-arriving event state in windowed aggregations | Streaming aggregates |
| **HLL approximation** | `approx_count_distinct` stands in for `countDistinct`, which Structured Streaming does not support | Streaming aggregates |
| **Row-local nearest airport** | Airport list broadcast as an array constant, Haversine distance via higher-order functions — no 20× expansion, no shuffle | DWD cleaning |
| **Interval recompute** | Every batch job takes `--start/--end`, snaps to window boundaries and `replaceWhere`s exactly that range: the same interval always produces the same rows | DWS |
| **Scoped MERGE deletes** | `WHEN NOT MATCHED BY SOURCE` restricted to the recomputed interval, so a 15-minute run can retire stale rows without touching history | ADS |
| **Transaction-per-batch serving layer** | Three real-time tables upserted in one PostgreSQL transaction; lake and serving layer stay consistent across replays | Streaming |
| **`max_active_runs=1`** | Prevents concurrent DAG runs from overwriting each other | Orchestration |
| **Quality gate** | Strong rules stop the DAG before ADS, so a bad interval never reaches the dashboards | DQ |
| **Alert engine with dedupe** | P1/P2 rules over the serving layer, one open alert per key, 30-minute cooldown | Alerting |
| **LSTM autoencoder** | Unsupervised sequence anomaly detection scored by reconstruction error | Anomaly |
| **Circular difference** | Normalises heading deltas across the 0°/360° boundary | Anomaly |

---

## Engineering Challenges Solved

Real problems encountered and fixed during development.

1. **Authentication broke mid-project.**
   OpenSky retired username/password authentication in March 2026 in favour of OAuth2 client-credentials. Reworked the producer around a token manager that fetches a bearer token, tracks its ~1800-second lifetime, and refreshes ahead of expiry so long-running polls never fail on a stale token.

2. **A daily quota that a naive poller burns through by noon.**
   The free tier allows 4,000 requests/day. Polling globally at a short interval exhausts it quickly. Constraining the query to a European bounding box and tuning the poll interval brought steady-state usage to ~2,880 requests/day while keeping data roughly 30 seconds fresh.
   *Lesson: rate limits are a design input, not an afterthought.*

3. **Distinct counts are not supported on streams.**
   Writing `countDistinct` in the streaming job fails at analysis time ("Distinct aggregations are not supported on streaming DataFrames"). Switched the streaming path to `approx_count_distinct`, whose HyperLogLog sketches are fixed-size, while the batch path keeps exact counts. Window state is kept bounded by the watermark, not by the sketch.
   *Lesson: streaming and batch can share logic, but not always the same aggregate functions.*

4. **Default shuffle partitions dominated runtime.**
   Spark's default of 200 shuffle partitions over-partitions a 4.1M-row workload (the collected data scaled 30x) — per-task scheduling overhead outweighed the actual work. Hand-tuning to 8 gave a 2.9x speedup; enabling AQE reached 2.6x automatically, which is the more maintainable choice as data volume changes.

5. **Keeping scheduled runs from overlapping.**
   silver and gold are overwritten on every run, so two overlapping DAG runs would write over each other. `max_active_runs=1` keeps one run at a time, and overwrite plus idempotent Delta `MERGE` writes make any single run safe to retry.

6. **Python 3.12 incompatibility in the Kafka client.**
   The long-standing `kafka-python` package does not support Python 3.12+. Switched to the maintained `kafka-python-ng` fork.

7. **Heading is a circular quantity, and differencing it naively poisoned the features.**
   `true_track` is reported in 0–360 degrees. A turn from 359° to 1° is +2 degrees, but plain subtraction yields −358 — so every crossing of due north registered as a violent manoeuvre and dominated the turn-rate feature. Normalising the difference to (−180, 180] fixed it.
   *Lesson: angles, timestamps and any other wrapping quantity need their own difference operator.*

8. **Duplicates after a checkpoint reset.**
   Re-running the bronze stream with a fresh checkpoint re-consumes Kafka from `earliest`, so bronze ends up with more than one row per `(icao24, event_time)`. Every consumer has to be safe against that: the state detail keeps one row per key, the alert table deduplicates on its `MERGE` key before merging, and `ml_features.py` deduplicates again, because in the feature path a duplicate divides by a zero sampling interval and fails quietly instead of loudly.
   *Lesson: exactly-once holds only within one checkpoint; every downstream step still needs its own idempotency.*

9. **A 2-hour latency test that measured nothing.**
   The first acceptance run started the sampler but the real-time job was stopped 20 minutes later. The sampler kept running, so freshness climbed to 896 seconds and the P95 was meaningless. The run was discarded and repeated with a guard: the report only counts samples where the data clock is advancing, and the run is only accepted if the sampler reaches its planned end with the pipeline alive.
   *Lesson: a measurement harness needs a validity check of its own.*

10. **The real-time job was OOM-killed mid-acceptance, and the exit code said 0.**
    At 375 aircraft per snapshot the container's 4 GB cgroup limit killed the JVM (`memory.events` reported `oom_kill 1`). The job was launched as `bash -c "spark-submit … | tee log"`, and bash returns the status of the last pipeline stage — `tee` — so the wrapper reported success while the job was gone. The heap itself was only 1.3 GB of a 2 GB maximum; anonymous memory had grown to 4.6 GB because glibc creates one malloc arena per thread. Raising the limit to 5 GB and setting `MALLOC_ARENA_MAX=2` kept the same workload between 2.4 and 3.4 GB for the next two hours.
    *Lesson: measure the memory the container actually holds, not the heap the JVM reports — and never read a pipeline's exit code as the job's.*

---

## Known Limitations

- The streaming window aggregates (`gold_overview_stream`) are written to Delta but not served; the dashboards follow the real-time serving layer and the 15-minute batch path.
- PostgreSQL statistics tables are refreshed with a full JDBC overwrite, which is not atomic: a dashboard query during the write can see an empty table. A staging table plus a transactional swap would fix it. The real-time tables do not have this problem — they are upserted inside a transaction.
- Kafka runs as a single broker with an auto-created single partition, so the replication factor is 1, `acks=all` is effectively `acks=1`, and there is no consumer parallelism.
- Bronze is written every 5 seconds and its small files are not compacted yet (Delta `OPTIMIZE`); the file count is the main driver of table-listing cost at this scale.
- The alert engine runs as a plain polling process. It is restart-safe (alerts are stored in PostgreSQL) but it is not clustered: running two instances would double-notify unless the dedupe table is used as the lock.
- Metabase dashboards that referenced the pre-M4 column names (`flight_cnt`, `total_flights`) need to be repointed to `aircraft_cnt`; Grafana cards are unaffected.

---

## License

For learning and portfolio purposes only. Flight data © OpenSky Network.
