"""
状态明细的清洗规则：流批共用的唯一实现
======================================
清洗只在这里写一次，三个作业共用：

  dwd_state.py        流式写 dwd_flight_state，回刷和对账也用它
  dwd_silver.py       批量写 silver_flights（过渡期保留，只取空中记录）
  dws_gold_stream.py  流式 5 分钟总览，只用有效性规则和派生字段

口径以 docs/requirements.md「维度与通用规则 → 清洗与去重」为准：改规则先改文档，再改这里。

★ 为什么能保证"流式结果 = 批量重算结果"
  1. 先过滤、后去重。反过来先去重的话，一条无效记录可能在批量重算里"占住"主键，
     把同主键的有效记录挤掉；流式里这两条要是分在两个微批，有效记录又会被插进去，
     两边就对不上了。
  2. 去重保留 Kafka offset 最小的一条。采集器用 icao24 做 key，同一架飞机总进同一个
     分区，offset 越小越先到：流式"先到先得"，批量"取 offset 最小"，选中的是同一行。
  3. 只用 SQL 表达式，不用 Python UDF，也不读当前时间：同样的输入一定算出同样的输出。
     不用 UDF 还有两个好处：计算全在 JVM 里，不用在 Python 和 JVM 之间来回序列化；
     本模块也不用分发到 executor 上。

要求 Spark 会话时区是 UTC：dt 分区取的是 UTC 日期，而 DATE(event_time) 按会话时区换算。
"""

import csv
import os

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

AIRPORTS_CSV = os.getenv("AIRPORTS_CSV", "/opt/spark/work-dir/dim/airports.csv")

# 距最近机场不超过这个距离，标记为"机场附近"（near_airport）
NEAR_AIRPORT_KM = 50

# ================================================================
#  规则一：有效性。不满足的记录不进 DWD，计入过滤率
# ================================================================
VALID_STATE = """
    icao24 IS NOT NULL
    AND event_time IS NOT NULL
    -- 起降判定靠 on_ground 翻转，没有这个值的记录没法用
    AND on_ground IS NOT NULL
    AND latitude  IS NOT NULL AND latitude  BETWEEN -90  AND 90
    AND longitude IS NOT NULL AND longitude BETWEEN -180 AND 180
    -- 民航飞机不会飞到 15,000 m 以上；地面记录的气压高度常为空，空值放行
    AND (baro_altitude IS NULL OR baro_altitude BETWEEN -500 AND 15000)
    -- 400 m/s 约 1,440 km/h，超过基本是脏数据
    AND (velocity IS NULL OR velocity BETWEEN 0 AND 400)
"""

# ================================================================
#  规则二：派生字段
# ================================================================
SPEED_KMH = "ROUND(velocity * 3.6, 1)"              # m/s 换算成 km/h
ALTITUDE_M = "ROUND(baro_altitude, 0)"               # 气压高度，米
ALTITUDE_FT = "ROUND(baro_altitude * 3.28084, 0)"    # 航空业惯用英尺

# 航向角换算成八方位（0 = 正北，顺时针）
HEADING_DIR = """
    CASE
        WHEN true_track IS NULL                        THEN 'UNKNOWN'
        WHEN true_track >= 337.5 OR true_track < 22.5  THEN 'N'
        WHEN true_track <  67.5                        THEN 'NE'
        WHEN true_track < 112.5                        THEN 'E'
        WHEN true_track < 157.5                        THEN 'SE'
        WHEN true_track < 202.5                        THEN 'S'
        WHEN true_track < 247.5                        THEN 'SW'
        WHEN true_track < 292.5                        THEN 'W'
        ELSE 'NW'
    END
"""

# 地面记录记为 GROUND；空中按垂直速率判断，±1 m/s 的阈值沿用原规则
FLIGHT_PHASE = """
    CASE
        WHEN on_ground              THEN 'GROUND'
        WHEN vertical_rate IS NULL  THEN 'UNKNOWN'
        WHEN vertical_rate >  1.0   THEN 'CLIMBING'
        WHEN vertical_rate < -1.0   THEN 'DESCENDING'
        ELSE 'CRUISING'
    END
"""


def haversine_km(lat1: str, lon1: str, lat2: str, lon2: str) -> str:
    """两点间大圆距离（km）的 SQL 表达式。参数是列名或表达式，6371 是地球平均半径（km）。"""
    return f"""(6371 * 2 * ASIN(SQRT(
        POWER(SIN(RADIANS({lat2} - {lat1}) / 2), 2)
        + COS(RADIANS({lat1})) * COS(RADIANS({lat2}))
        * POWER(SIN(RADIANS({lon2} - {lon1}) / 2), 2)
    )))"""


# ================================================================
#  规则三：最近机场
# ================================================================
# 原来的写法是 CROSS JOIN 机场表，再用 ROW_NUMBER 取距离最小的一行：数据先膨胀成
# "记录数 × 机场数"，还要按 (icao24, event_time) shuffle 一次。
#
# 现在把二十来行的机场表做成一个数组常量 _airports，每条记录在行内用高阶函数
# transform 算出到每个机场的距离，再用 array_min 取最小。结构体按字段顺序比较：
# 先比距离，距离相同再比 IATA 代码，所以结果是确定的。
# 不膨胀、不 shuffle，流式微批和批量回刷都受益。
NEAREST_AIRPORT = f"""
    array_min(transform(_airports, a -> named_struct(
        'dist_km', {haversine_km('latitude', 'longitude', 'a.lat', 'a.lon')},
        'iata',    a.iata,
        'name',    a.name,
        'city',    a.city
    )))
"""

# ================================================================
#  dwd_flight_state 的列，顺序就是表结构
# ================================================================
# 主键 (icao24, event_time) 放在最前：Delta 默认只给前 32 列收集 min/max 统计，
# event_time 靠前，按时间过滤（MERGE 条件、回刷区间）时才能跳过无关文件。
DWD_STATE_COLUMNS = [
    "icao24",
    "event_time",                         # 快照时间（UTC），不是位置测量时间
    "COALESCE(NULLIF(TRIM(callsign), ''), 'UNKNOWN') AS callsign",
    "origin_country",                     # 注册国，不是出发地
    "on_ground",                          # 起降判定依赖它
    "squawk",                             # 应急代码 7500/7600/7700 依赖它
    "latitude",
    "longitude",
    f"{SPEED_KMH} AS speed_kmh",
    f"{ALTITUDE_M} AS altitude_m",
    f"{ALTITUDE_FT} AS altitude_ft",
    "vertical_rate",                      # m/s，正数爬升、负数下降
    f"{FLIGHT_PHASE} AS flight_phase",
    "true_track AS heading_deg",
    f"{HEADING_DIR} AS heading_dir",
    "_nearest.iata AS near_airport_iata",
    "_nearest.name AS near_airport_name",
    "_nearest.city AS near_airport_city",
    "ROUND(_nearest.dist_km, 1) AS airport_dist_km",
    # 用四舍五入后的距离判断，和同一行展示的 airport_dist_km 保持一致
    f"ROUND(_nearest.dist_km, 1) <= {NEAR_AIRPORT_KM} AS near_airport",
    "timestamp_seconds(time_position) AS position_time",     # 最近一次位置测量的时间
    "timestamp_seconds(last_contact) AS last_contact_time",  # 最近一次收到任何报文的时间
    "kafka_ts",                           # 写入 Kafka 的时间，算处理延迟用
    "ingest_time",                        # 写入 ODS 的时间
    "DATE(event_time) AS dt",             # UTC 日期分区
]

KEY_COLUMNS = ("icao24", "event_time")


def load_airports(path: str = AIRPORTS_CSV) -> list:
    """读机场维表。只有几十行，直接在 driver 上用 csv 模块读，不用起 Spark 作业。"""
    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError(f"机场维表是空的：{path}")

    airports = [
        {
            "iata": r["iata"].strip(),
            "name": r["airport_name"].strip(),
            "city": r["city"].strip(),
            "lat": float(r["latitude"]),
            "lon": float(r["longitude"]),
        }
        for r in rows
    ]
    codes = [a["iata"] for a in airports]
    if len(set(codes)) != len(codes):
        raise ValueError(f"机场维表里有重复的 IATA 代码：{path}")
    return airports


def airports_column(airports: list) -> Column:
    """把机场列表做成数组常量，供 NEAREST_AIRPORT 在每一行里计算。"""
    return F.array(*[
        F.struct(
            F.lit(a["iata"]).alias("iata"),
            F.lit(a["name"]).alias("name"),
            F.lit(a["city"]).alias("city"),
            F.lit(a["lat"]).alias("lat"),
            F.lit(a["lon"]).alias("lon"),
        )
        for a in airports
    ])


def valid_states(df: DataFrame) -> DataFrame:
    """规则一：只留有效记录。流式 DataFrame 也能用。"""
    return df.where(VALID_STATE)


def dedupe_states(df: DataFrame) -> DataFrame:
    """同一 (icao24, event_time) 只留 Kafka offset 最小的一条。

    要用到 Kafka 元数据列 partition、offset，所以输入必须是 ODS 记录。
    窗口函数不能直接用在流式 DataFrame 上，流式作业要在 foreachBatch 里调用。
    """
    first_arrival = Window.partitionBy(*KEY_COLUMNS).orderBy("partition", "offset")
    return (
        df.withColumn("_rn", F.row_number().over(first_arrival))
        .where("_rn = 1")
        .drop("_rn")
    )


def clean_states(ods: DataFrame, airports: Column) -> DataFrame:
    """ODS 记录 → DWD 状态明细：过滤 → 去重 → 派生字段和最近机场。"""
    _require_utc(ods)
    return (
        dedupe_states(valid_states(ods))
        .withColumn("_airports", airports)
        .withColumn("_nearest", F.expr(NEAREST_AIRPORT))
        .selectExpr(*DWD_STATE_COLUMNS)
    )


def _require_utc(df: DataFrame) -> None:
    tz = df.sparkSession.conf.get("spark.sql.session.timeZone")
    if tz not in ("UTC", "Etc/UTC", "GMT", "Z", "+00:00"):
        raise ValueError(
            f"dt 按 UTC 日期分区，Spark 会话时区必须是 UTC，现在是 {tz}。"
            "在 SparkSession 上设置 spark.sql.session.timeZone=UTC。"
        )
