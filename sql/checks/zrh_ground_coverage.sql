-- ZRH 地面覆盖检查（M0）：OpenSky 在苏黎世机场能不能看到地面上的飞机，
-- 按 on_ground 翻转判定起降行不行得通。两段结果都要看，跑法见 sql/README.md。

-- 1. ZRH 5 km 内的原始记录，按 on_ground 分组
--    on_ground = true 的飞机数接近 0，说明这里收不到地面报文，起降事件要改用推断规则：
--    最后一个点在机场附近、低空且正在下降，之后长时间没有新数据。
SELECT on_ground,
       COUNT(*) AS records,
       COUNT(DISTINCT icao24) AS aircraft
FROM delta.`/data/delta/bronze_flights`
WHERE 6371 * 2 * ASIN(SQRT(
        POWER(SIN(RADIANS(latitude - 47.4647) / 2), 2)
        + COS(RADIANS(47.4647)) * COS(RADIANS(latitude))
        * POWER(SIN(RADIANS(longitude - 8.54917) / 2), 2))) <= 5
GROUP BY on_ground;

-- 2. 按需求文档的起降判定规则，数 ZRH 每天判得出多少次起降
--    规则：同一架飞机相邻两条记录 on_ground 翻转，间隔不超过 1 分钟，翻转后那条距 ZRH ≤ 5 km。
--    有地面记录不代表判得出起降：地面点太稀时，翻转前后两条常常隔了 1 分钟以上，
--    这样的翻转记在最后一列，不算起降。
--    读 dwd_flight_state（已清洗去重、含地面记录）。7 月的数据是 30 秒轮询，10 月起是 10 秒。
WITH states AS (
    SELECT dt, on_ground, near_airport_iata, airport_dist_km,
           LAG(on_ground) OVER w AS prev_on_ground,
           unix_timestamp(event_time) - unix_timestamp(LAG(event_time) OVER w) AS gap_s
    FROM delta.`/data/delta/dwd_flight_state`
    WINDOW w AS (PARTITION BY icao24 ORDER BY event_time)
)
SELECT dt,
       SUM(CASE WHEN gap_s <= 60 AND on_ground     THEN 1 ELSE 0 END) AS landings,
       SUM(CASE WHEN gap_s <= 60 AND NOT on_ground THEN 1 ELSE 0 END) AS takeoffs,
       SUM(CASE WHEN gap_s > 60                    THEN 1 ELSE 0 END) AS flips_over_1min
FROM states
WHERE on_ground <> prev_on_ground
  AND near_airport_iata = 'ZRH'
  AND airport_dist_km <= 5
GROUP BY dt
ORDER BY dt;
