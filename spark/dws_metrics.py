"""
DWS 指标口径：5 分钟机场流量、小时起降、国家占比、全区域总览
==============================================================
口径以 docs/requirements.md「指标口径」为准：改口径先改文档，再改这里。
这里只有计算，不读写 Delta，所以本地不装 Delta 也能直接测（和 flight_legs.py 一样）。

和 M3 之前 gold_* 表的差别（M4 改造）：

  1. 进近、离场不再用"50 km 内、在下降/爬升"这个粗口径，改成需求文档的定义：
       30 km 内、气压高度 ≤ 3,000 m、垂直速率 ≤ −1 m/s（离场 ≥ 1 m/s）。
     旧口径会把 50 km 外高空路过的下降飞机也算成进近，这是需求文档点名的错误。

  2. flight_cnt / total_flights 改名为 aircraft_cnt（架数）。
     这两个字段一直算的就是去重飞机数，叫"航班数"会让人以为是架次。

  3. 新增 dws_airport_hourly：按机场和 UTC 小时统计起飞、落地架次，来源是起降事件表，
     不是状态明细。架次只能数事件，数记录会把一架飞机在一小时里的几十条快照都算进去。

计算范围：调用方只传空中记录（on_ground = false）。地面记录不进 DWS：
停着的飞机不是"在飞"，而机场流量看的是空中交通。
"""

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F

# ================================================================
#  口径常量（改这里等于改指标，先改 docs/requirements.md）
# ================================================================
WINDOW_MINUTES = 5           # 5 分钟滚动窗口

APPROACH_RADIUS_KM = 30.0    # 进近/离场：距最近机场不超过 30 km
APPROACH_MAX_ALT_M = 3000.0  # ……气压高度不超过 3,000 m
APPROACH_VERTICAL_RATE = 1.0  # ……垂直速率 ≤ −1 m/s 算进近，≥ 1 m/s 算离场
NEAR_AIRPORT_KM = 50.0       # "机场附近"：距最近机场不超过 50 km（沿用原口径）

# 维表（dim/airports.csv）里有 22 个机场，其中 VIE、SZG 在采集范围（北纬 43–52°、
# 东经 2.5–12.5°）之外，只用于给边缘位置算方位。机场级指标只统计范围覆盖的 20 个，
# 否则它们的"最近机场"会把流量排名拉偏（约 0.3% 的位置）。
OUT_OF_COVERAGE = frozenset({"VIE", "SZG"})

AIRPORT_COLUMNS = [
    "window_start", "window_end", "airport_iata", "airport_city",
    "aircraft_cnt", "record_cnt", "avg_altitude_m", "avg_speed_kmh",
    "near_airport_cnt", "arriving_cnt", "departing_cnt", "dt",
]

HOURLY_COLUMNS = [
    "hour_start", "hour_end", "airport_iata", "airport_city",
    "takeoff_cnt", "landing_cnt", "movement_cnt", "dt",
]

COUNTRY_COLUMNS = [
    "window_start", "window_end", "origin_country",
    "aircraft_cnt", "avg_altitude_m", "avg_speed_kmh",
    "pct_of_window", "rank_in_window", "dt",
]

OVERVIEW_COLUMNS = [
    "window_start", "window_end", "aircraft_cnt", "country_cnt",
    "avg_altitude_m", "avg_speed_kmh", "max_speed_kmh", "max_altitude_m",
    "climbing_cnt", "descending_cnt", "cruising_cnt", "near_airport_cnt", "dt",
]

# ================================================================
#  口径一：进近 / 离场
# ================================================================
# 三个条件必须同时满足：离得近、够低、真的在下降。
# 只看"在下降"会把 10,000 m 高空路过的下降航班算进进近；只看"离得近"会把正在
# 爬升离场的飞机算进来。垂直速率为空（刚起飞还没报）不算，宁可少算不可误算。
#
# 注意：这些条件要等 SparkSession 建好之后才能构造 Column（模块导入时还没有会话，
# F.col 会断言失败），所以写成函数，在每个聚合里现场构造。
def _near_enough():
    return F.col("airport_dist_km") <= APPROACH_RADIUS_KM


def _low_enough():
    return F.col("altitude_m") <= APPROACH_MAX_ALT_M


def _arriving():
    return F.col("vertical_rate") <= -APPROACH_VERTICAL_RATE


def _departing():
    return F.col("vertical_rate") >= APPROACH_VERTICAL_RATE


def _window(col: str = "event_time"):
    """5 分钟滚动窗口。窗口起点对齐到整 5 分钟，重算幂等。"""
    return F.window(col, f"{WINDOW_MINUTES} minutes")


def airport_5min(states: DataFrame) -> DataFrame:
    """各机场 5 分钟窗口流量。

    一行 = 一个机场（按最近机场归属）一个 5 分钟窗口。
    aircraft_cnt 是这一窗口里出现过、且最近机场是这个机场的去重飞机数，
    和指标口径里的"5 分钟进近架数"一起看：后者只数低空下降的那部分。
    """
    w = _window().alias("_w")
    return (
        states
        # 只保留采集范围覆盖的机场：维表里还有 VIE、SZG 两个范围外的机场，
        # 只用来给边缘位置算方位；它们出现在流量排名里会让口径失真（约 0.3% 的位置）。
        .where(~F.col("near_airport_iata").isin(*OUT_OF_COVERAGE))
        .groupBy(w, "near_airport_iata", "near_airport_city")
        .agg(
            F.countDistinct("icao24").alias("aircraft_cnt"),
            F.count(F.lit(1)).alias("record_cnt"),
            F.round(F.avg("altitude_m"), 0).alias("avg_altitude_m"),
            F.round(F.avg("speed_kmh"), 1).alias("avg_speed_kmh"),
            F.countDistinct(
                F.when(F.col("airport_dist_km") <= NEAR_AIRPORT_KM, F.col("icao24"))
            ).alias("near_airport_cnt"),
            F.countDistinct(
                F.when(_near_enough() & _low_enough() & _arriving(), F.col("icao24"))
            ).alias("arriving_cnt"),
            F.countDistinct(
                F.when(_near_enough() & _low_enough() & _departing(), F.col("icao24"))
            ).alias("departing_cnt"),
        )
        .select(
            F.col("_w.start").alias("window_start"),
            F.col("_w.end").alias("window_end"),
            F.col("near_airport_iata").alias("airport_iata"),
            F.col("near_airport_city").alias("airport_city"),
            "aircraft_cnt", "record_cnt", "avg_altitude_m", "avg_speed_kmh",
            "near_airport_cnt", "arriving_cnt", "departing_cnt",
            F.to_date(F.col("_w.start")).alias("dt"),
        )
    )


def airport_hourly(events: DataFrame) -> DataFrame:
    """各机场按 UTC 小时的起降架次（数据来自 dwd_airport_event）。

    架次和架数是两个东西：一架飞机一小时里可能落一次、起两次，那是 3 架次、1 架飞机。
    事件表一行就是一次起飞或落地，所以这里直接数行，不再去重。
    """
    hour = F.date_trunc("hour", F.col("event_time")).alias("hour_start")
    return (
        events
        # 同 airport_5min：范围外的机场（VIE、SZG）不出小时起降指标
        .where(~F.col("airport_iata").isin(*OUT_OF_COVERAGE))
        .groupBy(hour, "airport_iata", "airport_city")
        .agg(
            F.sum(F.when(F.col("event_type") == "TAKEOFF", 1).otherwise(0))
                .cast("bigint").alias("takeoff_cnt"),
            F.sum(F.when(F.col("event_type") == "LANDING", 1).otherwise(0))
                .cast("bigint").alias("landing_cnt"),
            F.count(F.lit(1)).cast("bigint").alias("movement_cnt"),
        )
        .select(
            "hour_start",
            (F.col("hour_start") + F.expr("INTERVAL 1 HOUR")).alias("hour_end"),
            "airport_iata", "airport_city",
            "takeoff_cnt", "landing_cnt", "movement_cnt",
            F.to_date("hour_start").alias("dt"),
        )
    )


def country_5min(states: DataFrame) -> DataFrame:
    """各国 5 分钟窗口的飞机数、占比和排名。"""
    w = _window().alias("_w")
    base = (
        states
        .groupBy(w, "origin_country")
        .agg(
            F.countDistinct("icao24").alias("aircraft_cnt"),
            F.round(F.avg("altitude_m"), 0).alias("avg_altitude_m"),
            F.round(F.avg("speed_kmh"), 1).alias("avg_speed_kmh"),
        )
    )
    return (
        base
        .withColumn(
            "pct_of_window",
            F.round(100.0 * F.col("aircraft_cnt")
                    / F.sum("aircraft_cnt").over(Window.partitionBy("_w")), 2),
        )
        .withColumn(
            "rank_in_window",
            F.rank().over(Window.partitionBy("_w").orderBy(F.col("aircraft_cnt").desc())),
        )
        .select(
            F.col("_w.start").alias("window_start"),
            F.col("_w.end").alias("window_end"),
            "origin_country", "aircraft_cnt", "avg_altitude_m", "avg_speed_kmh",
            "pct_of_window", "rank_in_window",
            F.to_date(F.col("_w.start")).alias("dt"),
        )
    )


def overview_5min(states: DataFrame) -> DataFrame:
    """全区域 5 分钟总览：在飞架数、各国飞机数、各飞行阶段架数。"""
    w = _window().alias("_w")
    return (
        states
        .groupBy(w)
        .agg(
            F.countDistinct("icao24").alias("aircraft_cnt"),
            F.countDistinct("origin_country").alias("country_cnt"),
            F.round(F.avg("altitude_m"), 0).alias("avg_altitude_m"),
            F.round(F.avg("speed_kmh"), 1).alias("avg_speed_kmh"),
            F.round(F.max("speed_kmh"), 1).alias("max_speed_kmh"),
            F.round(F.max("altitude_m"), 0).alias("max_altitude_m"),
            F.countDistinct(F.when(F.col("flight_phase") == "CLIMBING", F.col("icao24")))
                .alias("climbing_cnt"),
            F.countDistinct(F.when(F.col("flight_phase") == "DESCENDING", F.col("icao24")))
                .alias("descending_cnt"),
            F.countDistinct(F.when(F.col("flight_phase") == "CRUISING", F.col("icao24")))
                .alias("cruising_cnt"),
            F.countDistinct(
                F.when(F.col("airport_dist_km") <= NEAR_AIRPORT_KM, F.col("icao24"))
            ).alias("near_airport_cnt"),
        )
        .select(
            F.col("_w.start").alias("window_start"),
            F.col("_w.end").alias("window_end"),
            "aircraft_cnt", "country_cnt", "avg_altitude_m", "avg_speed_kmh",
            "max_speed_kmh", "max_altitude_m",
            "climbing_cnt", "descending_cnt", "cruising_cnt", "near_airport_cnt",
            F.to_date(F.col("_w.start")).alias("dt"),
        )
    )


# ================================================================
#  时间边界：区间按窗口边界对齐
# ================================================================
# 按区间重算必须是幂等的：同样的 [start, end) 跑几次，写出的行都一样。
# 两个做法保证这一点：
#   1. 区间先向下对齐到窗口边界（5 分钟或 1 小时），区间里的窗口都是完整的；
#   2. 写表用 replaceWhere 覆盖同一区间，而不是追加。
# 不对齐就麻烦了：13:02 开始算，13:00 那个窗口只含后半段，和下次从 13:05 开始算的
# 同一窗口行数对不上，幂等就没了。
def floor_minutes(text: str, minutes: int) -> str:
    """把 'yyyy-MM-dd HH:mm:ss' 向下取整到 minutes 的整数倍。纯字符串运算，不涉及时区。"""
    date_part, clock = text.strip().split(" ")
    hh, mm, ss = (int(x) for x in clock.split(":"))
    total = hh * 60 + mm
    floored = (total // minutes) * minutes
    return f"{date_part} {floored // 60:02d}:{floored % 60:02d}:00"


def floor_hours(text: str) -> str:
    """把 'yyyy-MM-dd HH:mm:ss' 向下取整到整点。"""
    return f"{text.strip()[:13]}:00:00"
