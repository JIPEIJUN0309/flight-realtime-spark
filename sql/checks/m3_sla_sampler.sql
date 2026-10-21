-- M3 验收采样：每 10 秒采一次新鲜度和处理延迟，每 0.2 秒查一次服务层有没有半批或空表，到时自动结束。
-- 在 postgres 容器里用 psql 跑，不用装别的。先启动 rt_pipeline.py（它会建好 rt_atomicity_check 等视图），
-- 再另开一个窗口：
--   docker cp sql postgres:/tmp/
--   docker exec -e PGAPPNAME=m3_sampler postgres psql -U flight -d flightdb -v run=m3_2h -v duration="2 hours" -f /tmp/sql/checks/m3_sla_sampler.sql
-- 提前结束：
--   docker exec postgres psql -U flight -d flightdb -c "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name = 'm3_sampler'"
-- 汇总：sql/checks/m3_sla_report.sql。时间都按 UTC 存。

\set ON_ERROR_STOP on
\if :{?run}
\else
    \set run m3
\endif
\if :{?duration}
\else
    \set duration '2 hours'
\endif

CREATE TABLE IF NOT EXISTS rt_sla_run (
    run_id      text PRIMARY KEY,
    started_at  timestamptz NOT NULL,
    planned_end timestamptz NOT NULL,
    ended_at    timestamptz
);

-- 每 10 秒一个样本
CREATE TABLE IF NOT EXISTS rt_sla_sample (
    run_id             text NOT NULL,
    sampled_at         timestamptz NOT NULL,
    freshness_s        double precision,     -- 数据新鲜度：当前时间 - 最新状态表里最新的 event_time
    last_sync_seq      bigint,               -- 采样时最近一次同步
    last_batch_id      bigint,
    last_latency_max_s double precision,     -- 那一批的处理延迟：写入服务层的时间 - 最早一条的 kafka_ts
    last_latency_p50_s double precision,
    PRIMARY KEY (run_id, sampled_at)
);

-- 每 0.2 秒一次原子性探测：rt_atomicity_check 是一条 SELECT，几张表看到的是同一个快照
CREATE TABLE IF NOT EXISTS rt_probe_log (
    run_id        text NOT NULL,
    probed_at     timestamptz NOT NULL,
    seq           bigint,
    latest_rows   bigint,
    leg_rows      bigint,
    event_rows    bigint,
    seq_complete  boolean,      -- 标着最新同步序号的行数 = 那次同步记下的改动行数
    no_newer_rows boolean,      -- 没有比最新序号更新的行
    PRIMARY KEY (run_id, probed_at)
);

INSERT INTO rt_sla_run (run_id, started_at, planned_end)
VALUES (:'run', clock_timestamp(), clock_timestamp() + :'duration'::interval)
ON CONFLICT (run_id) DO UPDATE
SET started_at = EXCLUDED.started_at, planned_end = EXCLUDED.planned_end, ended_at = NULL;

-- DO 块里读不到 psql 变量，借会话参数传进去
SELECT set_config('m3.run', :'run', false) AS run_id,
       to_char(planned_end AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS planned_end_utc
FROM rt_sla_run WHERE run_id = :'run';

DO $$
DECLARE
    v_run  text := current_setting('m3.run');
    v_end  timestamptz;
    v_next timestamptz := clock_timestamp();
BEGIN
    SELECT planned_end INTO v_end FROM rt_sla_run WHERE run_id = v_run;
    WHILE clock_timestamp() < v_end LOOP
        INSERT INTO rt_probe_log
        SELECT v_run, clock_timestamp(), seq, latest_rows, leg_rows, event_rows, seq_complete, no_newer_rows
        FROM rt_atomicity_check;

        IF clock_timestamp() >= v_next THEN
            INSERT INTO rt_sla_sample
            SELECT v_run, clock_timestamp(),
                   EXTRACT(EPOCH FROM clock_timestamp() - (SELECT max(event_time) FROM ads_latest_position)),
                   l.sync_seq, l.batch_id, l.latency_max_s, l.latency_p50_s
            FROM (SELECT 1) AS one
            LEFT JOIN LATERAL (
                SELECT * FROM rt_sync_log WHERE kind = 'batch' ORDER BY sync_seq DESC LIMIT 1
            ) AS l ON TRUE;
            v_next := v_next + INTERVAL '10 seconds';
        END IF;

        COMMIT;                       -- 每次探测单独提交，下一次拿新的快照
        PERFORM pg_sleep(0.2);
    END LOOP;
    UPDATE rt_sla_run SET ended_at = clock_timestamp() WHERE run_id = v_run;
    COMMIT;
END
$$;

SELECT 'done' AS sampler, count(*) AS samples FROM rt_sla_sample WHERE run_id = :'run';
