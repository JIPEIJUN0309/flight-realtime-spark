-- 落地事件抽查（M2 验收）：判出的落地准不准、抖动还剩多少、漏了多少、丢失的航段是怎么丢的。
-- 五段都要看，跑法见 sql/README.md。
-- "地面""空中"按起降规则修正后的状态：on_ground 为 false，但离机场 5 km 内、时速低于 100 km/h 的也算地面
-- （和 spark/flight_legs.py 的 GROUND_STATE 一致）。

-- 1. 抽 20 个落地事件，附上判断依据，逐个确认是不是真落地
--    真落地的样子：落地前最后一条空中记录高度很低、速度在 200–350 km/h；落地后两分钟内都在地面；
--    最后一列给出粗判，"可疑"的再用呼号和 UTC 时间去航班记录网站核对。
--    按 (icao24, 事件时间) 的哈希排序取前 20 个：样本随数据变化，但同一份数据每次抽到的一样。
WITH pick AS (
    SELECT *
    FROM delta.`/data/delta/dwd_airport_event`
    WHERE event_type = 'LANDING'
    ORDER BY xxhash64(icao24, event_time)
    LIMIT 20
),
around AS (
    SELECT p.icao24, p.event_time AS landing_time,
           s.event_time, s.altitude_m, s.speed_kmh,
           s.on_ground OR COALESCE(s.airport_dist_km <= 5.0 AND s.speed_kmh < 100.0, FALSE) AS is_ground
    FROM pick p
    JOIN delta.`/data/delta/dwd_flight_state` s
      ON s.icao24 = p.icao24
     AND s.event_time BETWEEN p.prev_event_time AND p.event_time + INTERVAL 2 MINUTES
),
features AS (
    SELECT p.callsign, p.icao24, p.airport_iata,
           date_format(p.event_time, 'yyyy-MM-dd HH:mm:ss') AS landing_utc,
           p.gap_s,
           p.airport_dist_km,
           MAX(CASE WHEN a.event_time = p.prev_event_time THEN a.altitude_m END) AS alt_before_m,
           MAX(CASE WHEN a.event_time = p.prev_event_time THEN a.speed_kmh END)  AS speed_before_kmh,
           p.speed_kmh                                                            AS speed_at_kmh,
           SUM(CASE WHEN a.event_time > p.event_time AND a.is_ground THEN 1 ELSE 0 END)     AS ground_after,
           SUM(CASE WHEN a.event_time > p.event_time AND NOT a.is_ground THEN 1 ELSE 0 END) AS air_after
    FROM pick p
    LEFT JOIN around a ON a.icao24 = p.icao24 AND a.landing_time = p.event_time
    GROUP BY p.callsign, p.icao24, p.airport_iata, p.event_time, p.prev_event_time,
             p.gap_s, p.airport_dist_km, p.speed_kmh
)
SELECT *,
       CASE WHEN alt_before_m < 1000 AND air_after = 0 THEN '像真落地' ELSE '可疑' END AS verdict
FROM features
ORDER BY landing_utc;

-- 2. 抖动：落地后 10 分钟内又"起飞"。现实中过站至少二三十分钟，这种基本是地面标记来回跳
WITH e AS (
    SELECT icao24, event_type, event_time, airport_iata,
           LEAD(event_type) OVER (PARTITION BY icao24 ORDER BY event_time) AS next_type,
           LEAD(event_time) OVER (PARTITION BY icao24 ORDER BY event_time) AS next_time
    FROM delta.`/data/delta/dwd_airport_event`
)
SELECT airport_iata,
       COUNT(*) AS landings,
       SUM(CASE WHEN next_type = 'TAKEOFF'
                 AND unix_timestamp(next_time) - unix_timestamp(event_time) <= 600
                THEN 1 ELSE 0 END) AS takeoff_within_10min
FROM e
WHERE event_type = 'LANDING'
GROUP BY airport_iata
ORDER BY landings DESC;

-- 3. 起飞事件的时速分布：正常起飞和"落地后 10 分钟内又起飞"分开看，校准滑行抖动的速度上限用。
--    修正后，落地后 10 分钟内的起飞应该只剩时速 100 km/h 以上的
WITH e AS (
    SELECT event_type, event_time, speed_kmh,
           LAG(event_type) OVER (PARTITION BY icao24 ORDER BY event_time) AS prev_type,
           LAG(event_time) OVER (PARTITION BY icao24 ORDER BY event_time) AS prev_time
    FROM delta.`/data/delta/dwd_airport_event`
)
SELECT CASE WHEN prev_type = 'LANDING'
             AND unix_timestamp(event_time) - unix_timestamp(prev_time) <= 600
            THEN 'within_10min_of_landing' ELSE 'normal' END AS takeoff_kind,
       COUNT(*) AS takeoffs,
       percentile_approx(speed_kmh, array(0.05, 0.25, 0.5, 0.75, 0.95)) AS speed_kmh_p05_p25_p50_p75_p95
FROM e
WHERE event_type = 'TAKEOFF'
GROUP BY 1
ORDER BY 1;

-- 4. 漏判估计：丢失原因为"疑似落地未识别"的航段，按最后一条空中记录的最近机场汇总。
--    est_miss_rate = 疑似漏判 ÷（判出的落地 + 疑似漏判），是漏检率的粗略估计；
--    低空覆盖差的机场，信号常在降到 1,000 m 以下之前就断了（记成信号丢失），这个估计会偏低
WITH landed AS (
    SELECT arrival_iata AS airport, COUNT(*) AS landed
    FROM delta.`/data/delta/dwd_flight_leg`
    WHERE status = 'LANDED'
    GROUP BY arrival_iata
),
missed AS (
    SELECT last_airport_iata AS airport, COUNT(*) AS likely_missed
    FROM delta.`/data/delta/dwd_flight_leg`
    WHERE lost_reason = 'SUSPECTED_LANDING'
    GROUP BY last_airport_iata
)
SELECT COALESCE(l.airport, m.airport) AS airport,
       COALESCE(l.landed, 0)          AS landed,
       COALESCE(m.likely_missed, 0)   AS likely_missed,
       ROUND(COALESCE(m.likely_missed, 0)
             / (COALESCE(l.landed, 0) + COALESCE(m.likely_missed, 0)), 3) AS est_miss_rate
FROM landed l
FULL OUTER JOIN missed m ON l.airport = m.airport
ORDER BY landed DESC, likely_missed DESC;

-- 5. 丢失原因分布，以及其中有多少是采集器停了造成的：航段最后一条空中记录之后，
--    超过 20 分钟没有任何一张快照（整个采集都停了），飞机并没有真的消失。
--    快照时间所有飞机共用，DISTINCT 之后只有几千个时刻
WITH snaps AS (
    SELECT event_time,
           LEAD(event_time) OVER (ORDER BY event_time) AS next_snap
    FROM (SELECT DISTINCT event_time FROM delta.`/data/delta/dwd_flight_state`)
)
SELECT l.lost_reason,
       COUNT(*) AS legs,
       SUM(CASE WHEN unix_timestamp(s.next_snap) - unix_timestamp(l.last_seen) > 1200
                THEN 1 ELSE 0 END) AS collection_stopped
FROM delta.`/data/delta/dwd_flight_leg` l
JOIN snaps s ON s.event_time = l.last_seen
WHERE l.status = 'LOST'
GROUP BY l.lost_reason
ORDER BY legs DESC;
