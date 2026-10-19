-- 采集范围核对：每天状态明细的经纬度范围，以及第一条落在旧范围（北纬 45–50°、东经 5–10°）外的记录。
-- 判断航段"飞出范围"要用记录当时的采集范围，范围的历史登记在 spark/flight_legs.py 的 COVERAGE。
-- 用这个查询核对登记得对不对：旧范围的日子 first_outside_old_utc 应该为空；换成新范围的那天，
-- 它应该和 first_utc 差不多（一开始就是新范围），否则说明当天中途才换，生效时间要改成那一刻。
-- 以后改了采集器的 BBOX_*，把下面的旧范围换成改之前的范围再查。
SELECT dt,
       COUNT(*)                                    AS records,
       date_format(MIN(event_time), 'HH:mm:ss')    AS first_utc,
       date_format(MAX(event_time), 'HH:mm:ss')    AS last_utc,
       ROUND(MIN(latitude), 2)                     AS lat_min,
       ROUND(MAX(latitude), 2)                     AS lat_max,
       ROUND(MIN(longitude), 2)                    AS lon_min,
       ROUND(MAX(longitude), 2)                    AS lon_max,
       date_format(MIN(CASE WHEN latitude < 45 OR latitude > 50 OR longitude < 5 OR longitude > 10
                            THEN event_time END), 'HH:mm:ss') AS first_outside_old_utc
FROM delta.`/data/delta/dwd_flight_state`
GROUP BY dt
ORDER BY dt;
