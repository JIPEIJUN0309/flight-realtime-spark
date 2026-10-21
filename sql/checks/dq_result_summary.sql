-- 质量门禁与告警的日常巡检：直接在 postgres 容器里跑
--   docker cp sql postgres:/tmp/
--   docker exec postgres psql -U flight -d flightdb -f /tmp/sql/checks/dq_result_summary.sql
-- dq_result 由 dq_checks.py 每次批处理写一份（Delta 里还留了历史），alert_event 由 alert_engine.py 写。
-- 时间都按 UTC 存。

\echo '== 1. 最近一次质量检查：哪些规则没过'
SELECT rule_code, level,
       CASE WHEN passed THEN '通过' ELSE '未通过' END AS result,
       CASE WHEN metric IS NOT NULL
            THEN round(metric::numeric, 4) || '（阈值 ' || round(threshold::numeric, 4) || '）'
            ELSE '-' END AS value,
       detail,
       to_char(checked_at AT TIME ZONE 'UTC', 'MM-DD HH24:MI:SS') AS checked_utc
FROM dq_result
WHERE checked_at = (SELECT max(checked_at) FROM dq_result)
ORDER BY level DESC, rule_code;

\echo '== 2. 最近 24 小时强规则失败次数（按规则）'
SELECT rule_code, count(*) AS failed_checks,
       to_char(max(checked_at) AT TIME ZONE 'UTC', 'MM-DD HH24:MI:SS') AS last_failed_utc
FROM dq_result
WHERE level = 'strong' AND NOT passed
  AND checked_at > now() - interval '24 hours'
GROUP BY rule_code
ORDER BY failed_checks DESC;

\echo '== 3. 当前未恢复的告警（P1 在前）'
SELECT level, title,
       regexp_replace(detail, '\s+', ' ', 'g') AS detail,
       to_char(fired_at AT TIME ZONE 'UTC', 'MM-DD HH24:MI:SS') AS fired_utc,
       round(EXTRACT(EPOCH FROM now() - fired_at) / 60) AS open_minutes
FROM alert_event
WHERE cleared_at IS NULL
ORDER BY level, fired_at;

\echo '== 4. 最近 24 小时的告警历史（含已恢复）'
SELECT to_char(fired_at AT TIME ZONE 'UTC', 'MM-DD HH24:MI') AS fired_utc,
       level, title,
       CASE WHEN cleared_at IS NULL THEN '未恢复'
            ELSE '已恢复（' || round(EXTRACT(EPOCH FROM cleared_at - fired_at)) || ' 秒）'
       END AS state
FROM alert_event
WHERE fired_at > now() - interval '24 hours'
ORDER BY fired_at DESC
LIMIT 30;

\echo '== 5. 应急代码告警（squawk 7500/7600/7700，按最近 24 小时）'
SELECT to_char(fired_at AT TIME ZONE 'UTC', 'MM-DD HH24:MI:SS') AS fired_utc,
       detail, CASE WHEN cleared_at IS NULL THEN '未恢复' ELSE '已恢复' END AS state
FROM alert_event
WHERE alert_key LIKE 'emergency:%'
  AND fired_at > now() - interval '24 hours'
ORDER BY fired_at DESC;
