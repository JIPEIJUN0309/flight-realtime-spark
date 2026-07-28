-- ============================================================
-- 阶段七 (3/3)：Hive 侧基准（Hive on Tez）
-- 和 spark/bench_spark.py 跑【同一段】聚合 SQL、读同一批 Parquet。
--
-- 跑法（先 up 一次让 /bench 挂载生效，再用 beeline -f 执行本文件）：
--   docker compose -f docker-compose.hive.yml up -d
--   docker exec -it flight-hive-server2 \
--       beeline -u jdbc:hive2://localhost:10000 -f /bench/bench_hive.sql
--
-- beeline 每条语句都会打印 "Time taken: X seconds" —— 那就是 Hive 的耗时。
-- 我们要的是第 3、4 步那两条聚合查询的 Time taken。
-- ============================================================

-- 1) 建外部表，指向 Spark 导出的 Parquet（两引擎读同一批物理文件）
CREATE EXTERNAL TABLE IF NOT EXISTS flights_small (
  icao24 STRING, origin_country STRING, event_ts BIGINT,
  altitude_m DOUBLE, speed_kmh DOUBLE,
  near_airport_iata STRING, near_airport_city STRING,
  near_airport BOOLEAN, flight_phase STRING
) STORED AS PARQUET
LOCATION 'file:///flight-data/bench/flights_small';

CREATE EXTERNAL TABLE IF NOT EXISTS flights_big (
  icao24 STRING, origin_country STRING, event_ts BIGINT,
  altitude_m DOUBLE, speed_kmh DOUBLE,
  near_airport_iata STRING, near_airport_city STRING,
  near_airport BOOLEAN, flight_phase STRING
) STORED AS PARQUET
LOCATION 'file:///flight-data/bench/flights_big';

-- 2) 行数确认（应为 小≈13.6万 / 大≈410万）
SELECT COUNT(*) AS small_rows FROM flights_small;
SELECT COUNT(*) AS big_rows   FROM flights_big;

-- ============================================================
-- 3) 计时 · 小份（13.6 万行）—— 记这条的 Time taken
-- ============================================================
SELECT COUNT(*) AS groups, ROUND(SUM(pct_of_window), 2) AS chk_pct, SUM(rank_in_window) AS chk_rank
FROM (
    SELECT
        window_start, origin_country, flight_cnt, record_cnt, avg_alt, avg_spd,
        climbing, descending, near_airport_cnt,
        ROUND(100.0 * flight_cnt / SUM(flight_cnt) OVER (PARTITION BY window_start), 2) AS pct_of_window,
        RANK() OVER (PARTITION BY window_start ORDER BY flight_cnt DESC) AS rank_in_window
    FROM (
        SELECT
            FLOOR(event_ts / 300) * 300 AS window_start,
            origin_country,
            COUNT(*) AS record_cnt,
            COUNT(DISTINCT icao24) AS flight_cnt,
            ROUND(AVG(altitude_m), 0) AS avg_alt,
            ROUND(AVG(speed_kmh), 1) AS avg_spd,
            SUM(CASE WHEN flight_phase = 'CLIMBING' THEN 1 ELSE 0 END) AS climbing,
            SUM(CASE WHEN flight_phase = 'DESCENDING' THEN 1 ELSE 0 END) AS descending,
            SUM(CASE WHEN near_airport THEN 1 ELSE 0 END) AS near_airport_cnt
        FROM flights_small
        GROUP BY FLOOR(event_ts / 300) * 300, origin_country
    ) agg
) t;

-- ============================================================
-- 4) 计时 · 大份（410 万行）—— 跑两次看稳定性，记 Time taken
-- ============================================================
SELECT COUNT(*) AS groups, ROUND(SUM(pct_of_window), 2) AS chk_pct, SUM(rank_in_window) AS chk_rank
FROM (
    SELECT
        window_start, origin_country, flight_cnt, record_cnt, avg_alt, avg_spd,
        climbing, descending, near_airport_cnt,
        ROUND(100.0 * flight_cnt / SUM(flight_cnt) OVER (PARTITION BY window_start), 2) AS pct_of_window,
        RANK() OVER (PARTITION BY window_start ORDER BY flight_cnt DESC) AS rank_in_window
    FROM (
        SELECT
            FLOOR(event_ts / 300) * 300 AS window_start,
            origin_country,
            COUNT(*) AS record_cnt,
            COUNT(DISTINCT icao24) AS flight_cnt,
            ROUND(AVG(altitude_m), 0) AS avg_alt,
            ROUND(AVG(speed_kmh), 1) AS avg_spd,
            SUM(CASE WHEN flight_phase = 'CLIMBING' THEN 1 ELSE 0 END) AS climbing,
            SUM(CASE WHEN flight_phase = 'DESCENDING' THEN 1 ELSE 0 END) AS descending,
            SUM(CASE WHEN near_airport THEN 1 ELSE 0 END) AS near_airport_cnt
        FROM flights_big
        GROUP BY FLOOR(event_ts / 300) * 300, origin_country
    ) agg
) t;

SELECT COUNT(*) AS groups, ROUND(SUM(pct_of_window), 2) AS chk_pct, SUM(rank_in_window) AS chk_rank
FROM (
    SELECT
        window_start, origin_country, flight_cnt, record_cnt, avg_alt, avg_spd,
        climbing, descending, near_airport_cnt,
        ROUND(100.0 * flight_cnt / SUM(flight_cnt) OVER (PARTITION BY window_start), 2) AS pct_of_window,
        RANK() OVER (PARTITION BY window_start ORDER BY flight_cnt DESC) AS rank_in_window
    FROM (
        SELECT
            FLOOR(event_ts / 300) * 300 AS window_start,
            origin_country,
            COUNT(*) AS record_cnt,
            COUNT(DISTINCT icao24) AS flight_cnt,
            ROUND(AVG(altitude_m), 0) AS avg_alt,
            ROUND(AVG(speed_kmh), 1) AS avg_spd,
            SUM(CASE WHEN flight_phase = 'CLIMBING' THEN 1 ELSE 0 END) AS climbing,
            SUM(CASE WHEN flight_phase = 'DESCENDING' THEN 1 ELSE 0 END) AS descending,
            SUM(CASE WHEN near_airport THEN 1 ELSE 0 END) AS near_airport_cnt
        FROM flights_big
        GROUP BY FLOOR(event_ts / 300) * 300, origin_country
    ) agg
) t;
