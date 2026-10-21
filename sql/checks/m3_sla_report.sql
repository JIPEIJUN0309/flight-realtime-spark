-- M3 验收汇总：新鲜度、处理延迟、每批耗时、服务层原子性。数据来自 m3_sla_sampler.sql 和实时作业写的日志表。
--   docker cp sql postgres:/tmp/
--   docker exec postgres psql -U flight -d flightdb -v run=m3_2h -f /tmp/sql/checks/m3_sla_report.sql
-- 通过标准（docs/requirements.md「验收方式」）：
--   新鲜度 P95 ≤ 30 秒；处理延迟 P95 ≤ 10 秒；每批耗时稳定小于 5 秒；探测期间 0 次半批、0 次空表

\set ON_ERROR_STOP on
\if :{?run}
\else
    \set run m3
\endif

\echo '== 1. 运行区间（UTC）'
SELECT run_id,
       to_char(started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS started_utc,
       to_char(COALESCE(ended_at, clock_timestamp()) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS ended_utc,
       round(EXTRACT(EPOCH FROM COALESCE(ended_at, clock_timestamp()) - started_at) / 60) AS minutes
FROM rt_sla_run WHERE run_id = :'run';

\echo '== 2. 端到端新鲜度（每 10 秒一个样本）：P95 ≤ 30 秒'
SELECT count(*) AS samples,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY freshness_s)::numeric, 1) AS p50_s,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY freshness_s)::numeric, 1) AS p95_s,
       round(max(freshness_s)::numeric, 1) AS max_s,
       count(*) FILTER (WHERE freshness_s > 30) AS over_30s
FROM rt_sla_sample WHERE run_id = :'run';

\echo '== 3. 处理延迟：P95 ≤ 10 秒。按批：区间内每个微批取最早一条记录的延迟；按采样：每 10 秒看最近一批'
SELECT 'by_batch' AS basis,
       count(*) AS n,
       sum(n_records) AS records,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY latency_max_s)::numeric, 1) AS p50_s,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_max_s)::numeric, 1) AS p95_s,
       round(max(latency_max_s)::numeric, 1) AS max_s,
       count(*) FILTER (WHERE latency_max_s > 10) AS over_10s
FROM rt_sync_log l
JOIN rt_sla_run r ON l.synced_at BETWEEN r.started_at AND COALESCE(r.ended_at, clock_timestamp())
WHERE r.run_id = :'run' AND l.kind = 'batch' AND l.n_records > 0
UNION ALL
SELECT 'by_sample', count(*), NULL,
       round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY last_latency_max_s)::numeric, 1),
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY last_latency_max_s)::numeric, 1),
       round(max(last_latency_max_s)::numeric, 1),
       count(*) FILTER (WHERE last_latency_max_s > 10)
FROM rt_sla_sample WHERE run_id = :'run';

\echo '== 4. 每批耗时（Spark 统计的整批时间）：稳定小于 5 秒'
SELECT count(*) AS batches,
       round(avg(num_input_rows)) AS avg_rows,
       max(num_input_rows) AS max_rows,
       percentile_disc(0.5)  WITHIN GROUP (ORDER BY trigger_ms) AS p50_ms,
       percentile_disc(0.95) WITHIN GROUP (ORDER BY trigger_ms) AS p95_ms,
       percentile_disc(0.99) WITHIN GROUP (ORDER BY trigger_ms) AS p99_ms,
       max(trigger_ms) AS max_ms,
       count(*) FILTER (WHERE trigger_ms >= 5000) AS over_5s,
       round(avg(compute_ms)) AS avg_compute_ms,
       round(avg(pg_ms)) AS avg_pg_ms,
       round(avg(delta_ms)) AS avg_delta_ms
FROM rt_batch_log b
JOIN rt_sla_run r ON b.trigger_at BETWEEN r.started_at AND COALESCE(r.ended_at, clock_timestamp())
WHERE r.run_id = :'run' AND b.num_input_rows > 0;

\echo '== 5. 服务层原子性：半批 0 次、空表 0 次'
SELECT count(*) AS probes,
       count(DISTINCT seq) AS syncs_seen,
       count(*) FILTER (WHERE NOT seq_complete OR NOT no_newer_rows) AS half_batch_seen,
       count(*) FILTER (WHERE latest_rows = 0 OR leg_rows = 0 OR event_rows = 0) AS empty_table_seen
FROM rt_probe_log WHERE run_id = :'run';

\echo '== 6. 新鲜度最差的 5 个样本（UTC），方便对照作业日志'
SELECT to_char(sampled_at AT TIME ZONE 'UTC', 'HH24:MI:SS') AS at_utc,
       round(freshness_s::numeric, 1) AS freshness_s,
       last_batch_id,
       round(last_latency_max_s::numeric, 1) AS latency_s
FROM rt_sla_sample WHERE run_id = :'run'
ORDER BY freshness_s DESC NULLS LAST
LIMIT 5;
