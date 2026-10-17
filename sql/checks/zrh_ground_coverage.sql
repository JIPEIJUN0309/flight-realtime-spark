-- 统计 ZRH 5 km 内的记录，看地面记录（on_ground = true）够不够支撑起降判定。
-- 如果 on_ground = true 的飞机数接近 0，起降事件要改用推断规则：
-- 最后一个点在机场附近、低空且正在下降，之后长时间没有新数据。
SELECT on_ground,
       COUNT(*) AS records,
       COUNT(DISTINCT icao24) AS aircraft
FROM delta.`/data/delta/bronze_flights`
WHERE 6371 * 2 * ASIN(SQRT(
        POWER(SIN(RADIANS(latitude - 47.4647) / 2), 2)
        + COS(RADIANS(47.4647)) * COS(RADIANS(latitude))
        * POWER(SIN(RADIANS(longitude - 8.54917) / 2), 2))) <= 5
GROUP BY on_ground;
