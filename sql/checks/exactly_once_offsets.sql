-- 验证 Kafka 到 ODS 恰好一次。
-- 用法：演练前记下开始时间（UTC），替换下面两处时间，在 spark-sql 里执行。
-- 只看演练时间段，因为历史上重置过 checkpoint 的数据本来就有重复。

-- 1. 重复：应返回 0 行
SELECT `partition`, `offset`, COUNT(*) AS cnt
FROM delta.`/data/delta/bronze_flights`
WHERE kafka_ts >= TIMESTAMP '2026-01-01 00:00:00'  -- 改成演练开始时间
GROUP BY `partition`, `offset`
HAVING COUNT(*) > 1;

-- 2. 断号：每个分区的 missing 应为 0
SELECT `partition`,
       MIN(`offset`) AS min_offset,
       MAX(`offset`) AS max_offset,
       COUNT(DISTINCT `offset`) AS distinct_offsets,
       MAX(`offset`) - MIN(`offset`) + 1 - COUNT(DISTINCT `offset`) AS missing
FROM delta.`/data/delta/bronze_flights`
WHERE kafka_ts >= TIMESTAMP '2026-01-01 00:00:00'  -- 改成演练开始时间
GROUP BY `partition`;
