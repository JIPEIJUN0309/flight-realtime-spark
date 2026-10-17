# Anomaly Detection — Implementation Notes

> Why this module exists, how the model is chosen and how it is evaluated are covered in the
> [README](../README.md#anomaly-detection). This document covers **running it** and the
> implementation decisions behind it.

---

## Prerequisites

### 1. Enough continuous track history

A sequence model needs *long continuous tracks per aircraft*, not a large total row count. Data accumulated from several short collection runs will not slice into windows.

Run the collector and the bronze stream together for 6–12 hours, over daylight hours in the bounding box (45–50°N / 5–10°E), where traffic is densest:

```bash
# terminal 1 — collector
python ingestion/opensky_producer.py

# terminal 2 — bronze streaming job
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/ods_bronze.py
```

Collection readiness is measured by **how many aircraft have enough points**, not by total row count: 100k rows spread over 5,000 aircraft yields almost no windows, while 50k rows over 800 aircraft is ample. `check_collection.py` reports that figure directly and can be run at any time without disturbing the running collector:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/check_collection.py
```

Aim for **300+ aircraft with 30 or more valid samples each**.

### 2. Rebuild the Spark image

The module adds PyTorch, scikit-learn, pandas and pyarrow to the Spark container:

```bash
docker compose build spark
docker compose up -d spark
```

The JAR-download layer is cached, so only the pip layer is rebuilt.

> PyTorch is installed from the CPU-only index (`download.pytorch.org/whl/cpu`). The default PyPI wheel for Linux bundles CUDA and pulls in roughly 2.5 GB of GPU libraries this container never uses. If that index is unreachable on your network, plain `pip install torch` works identically — the image is just much larger.

---

## Running

```bash
# 0. Rebuild silver from the freshly collected bronze data.
#    ml_features reads silver, not bronze — skipping this step
#    silently trains on whatever silver held from a previous run.
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dwd_silver.py

# 1. Extract per-aircraft track sequences from silver
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/ml_features.py

# 2. Train, including the latent-dimension sweep
docker exec -it spark python3 /opt/spark/work-dir/jobs/ml_train.py

# 3. Evaluate — three-way comparison, threshold selection, attribution
docker exec -it spark python3 /opt/spark/work-dir/jobs/ml_eval.py
```

Steps 2 and 3 use `python3` rather than `spark-submit`: they read Parquet directly and need no JVM.

That is everything needed to produce the evaluation numbers. To also publish alerts to the dashboard, run the serving chain:

```bash
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dws_gold_batch.py
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/ads_export.py
docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/ml_score.py
```

`ml_features` and `ml_score` are wired into the `flight_batch_pipeline` DAG after `ads_export`, so this sequence runs itself once the DAG is enabled. Training and evaluation are not — see [Training is not scheduled](#training-is-not-scheduled).

---

## Files

| File | Runs via | Responsibility |
|------|----------|----------------|
| `spark/check_collection.py` | `spark-submit` | Collection readiness — how many aircraft have enough samples yet |
| `spark/ml_common.py` | imported | Feature derivation, windowing, anomaly injection, model definition |
| `spark/ml_features.py` | `spark-submit` | silver → dedup + continuity filter → track sequences (Parquet) |
| `spark/ml_train.py` | `python3` | Windowing, latent sweep, final training |
| `spark/ml_eval.py` | `python3` | Three-way comparison, threshold and debounce selection, attribution |
| `spark/ml_score.py` | `spark-submit` | Inference → Delta `MERGE` → PostgreSQL |

### Outputs

```
data/ml/points/               Track sequences (Parquet)
data/ml/model/lstm_ae.pt      Model weights
data/ml/model/norm.npz        Per-feature normalisation statistics
data/ml/model/config.json     Hyperparameters, sweep results, evaluation metrics, alert threshold
```

All of `data/` is git-ignored. `ml_eval.py` writes its selected threshold back into `config.json`, which `ml_score.py` then reads — evaluation and serving cannot drift apart by hand-copying a number.

---

## Dashboard

`ml_score.py` writes `ads_ml_anomaly` to PostgreSQL. The panel worth building compares the two detectors on the same time axis:

```sql
SELECT
    CASE
        WHEN is_alert AND rule_alert     THEN 'both'
        WHEN is_alert AND NOT rule_alert THEN 'model only'
        ELSE 'rule only'
    END AS source,
    COUNT(*) AS cnt
FROM ads_ml_anomaly
GROUP BY 1;
```

"Model only" is the set of sequence patterns fixed thresholds cannot express. "Rule only" is where a single sample crossed a limit but the surrounding ten minutes were unremarkable.

---

## Implementation notes

### Feature derivation lives in pandas, not Spark SQL

It would be natural to derive features with Spark SQL window functions alongside the rest of the pipeline. It is deliberately not done that way.

The same derivation is needed in three places — training, evaluation and scoring — and evaluation additionally has to **re-derive features after injecting synthetic anomalies into raw columns**. Two implementations of the same logic drift, and when they do, the features computed offline stop matching the features computed at serving time. That is training-serving skew, and it fails silently.

So derivation exists once, in `ml_common.derive_features()`. Spark keeps the job it is actually better at: pulling detail out of the lakehouse, deduplicating it, and filtering aircraft by track continuity.

### Heading is a circular quantity

`true_track` is reported in 0–360 degrees. A turn from 359° to 1° is +2 degrees; plain subtraction gives −358. Left unhandled, every crossing of due north registers as a violent manoeuvre and dominates the turn-rate feature. `ml_common.circular_delta()` normalises the difference to (−180, 180].

### Duplicate rows must be removed before differencing

Re-running the bronze stream with a fresh checkpoint re-consumes Kafka, so silver can hold more than one row per `(icao24, event_time)`. Every feature divided by the sampling interval then divides by zero. The serving layer already hit this as a loud `MERGE` failure; in the feature path it fails quietly, which is worse. `ml_features.py` deduplicates explicitly.

### Gaps are not interpolated

A missing report *is* a signal — the `gap_ratio` feature exists to detect it. Interpolating across gaps would erase exactly what the feature is for. Windows containing missing samples are dropped whole instead.

### Anomalies are injected into raw columns

Synthetic anomalies modify altitude, speed, position and sampling continuity, and the derived features are then recomputed from scratch. Writing anomalies directly into derived features would hand the model the answer and produce meaningless scores.

### `MERGE` uses delete-on-missing semantics

Alerts are recomputed from the full source on every run, so the target must mirror the source rather than accumulate. `ml_score.py` passes `sync=True` to the serving layer's `MERGE` helper, which adds `whenNotMatchedBySourceDelete()`. Without it, changing the model or the threshold leaves the previous run's rows behind — the same failure the rule-based alert table hit when its threshold was recalibrated.

### Training is not scheduled

Inference reruns on every batch; training is an occasional model-iteration step that needs a human to read the sweep and decide whether to promote a new model. Scheduling it alongside inference would silently replace the production model several times an hour and make any regression impossible to trace back to a model version.

`ml_score` skips rather than fails when no trained model is present, so the rest of the chain is never blocked by it.

---

## Known limitations

Stated plainly, because they bound how the results should be read.

1. **Dataset scale.** This runs on hours of European-airspace traffic, not a production fleet-history archive. The comparison between methods is meaningful; the absolute scores are a method demonstration, not a benchmark.

2. **Synthetic anomalies are predefined shapes.** Four injected patterns cannot represent the space of real anomalies, so measured recall is optimistic. The false-positive regression set exists to bound the error in the other direction, but it does not close this gap.

3. **The regression set covers one failure mode.** It is built from climb-rate false positives, the one category with human-confirmed labels. Other over-firing modes are unmeasured.

4. **Inference is batch, not streaming.** Detection latency is the DAG interval. Nothing in the model prevents streaming inference; it simply is not wired that way.

5. **One model for all aircraft.** No conditioning on aircraft type or flight phase, though normal behaviour differs substantially across both. Per-phase models would likely reduce false positives — untested.
