"""
最新状态 ads_latest_position：流批共用的唯一实现
==============================================
一行 = 一架飞机最近一次出现时的状态（含地面记录），主键 icao24。
口径以 docs/requirements.md「指标口径」和「维度与通用规则 → 其他」为准：改规则先改文档，再改这里。

  数据时钟  已处理数据里最晚的快照时间（event_time），不是机器时间
  已离开    最后出现时间比数据时钟早 1 分钟以上，地图不显示
  当前      最后出现时间在数据时钟之前 30 秒以内；当前进近、当前离场都只数"当前"的飞机

★ 为什么按数据时钟，不按机器时间
  采集器停了，按机器时间一分钟后所有飞机都会"离开"、进近数变成 0，看起来像机场突然没飞机了；
  按数据时钟，看板停在最后一张快照，过期由新鲜度卡片提示。另外流式每批和事后全量重算用的是
  同一个时钟，结果才对得上（和 flight_legs 判航段超时的做法一样）。

★ 为什么判定只写在这里
  实时作业每批调用 latest_updates 算出要写的行（新出现的、位置更新的、刚变成已离开的），
  同一组行 MERGE 进 Delta、upsert 进 PostgreSQL。两边都只按主键写，不各自再写一遍判定，
  流式、全量重算和单测走的都是这一个函数。

要求 Spark 会话时区是 UTC：常量时间按 UTC 解释。
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# ================================================================
#  口径参数（和需求文档"待校准参数"一致）
# ================================================================
CURRENT_WINDOW_S = 30        # "当前"：最近 30 秒有更新
DEPARTED_AFTER_S = 60        # "已离开"：最后出现超过 1 分钟
APPROACH_RADIUS_KM = 30.0    # 进近、离场：距最近机场不超过它……
APPROACH_MAX_ALT_M = 3000.0  # ……气压高度不超过它……
VERTICAL_RATE_MS = 1.0       # ……垂直速率 ≤ −1 m/s 为进近，≥ 1 m/s 为离场

# 进近、离场的判定。Spark SQL 和 PostgreSQL 的写法相同：服务层的视图 rt_current_position 直接用这两个
# 表达式，看板只数视图里的标记，口径只在这一处。垂直速率或高度为空时结果为空，不计入
APPROACH = (f"(airport_dist_km <= {APPROACH_RADIUS_KM} AND altitude_m <= {APPROACH_MAX_ALT_M} "
            f"AND vertical_rate <= -{VERTICAL_RATE_MS})")
DEPARTURE = (f"(airport_dist_km <= {APPROACH_RADIUS_KM} AND altitude_m <= {APPROACH_MAX_ALT_M} "
             f"AND vertical_rate >= {VERTICAL_RATE_MS})")

# ads_latest_position 的列（不含写入时间），顺序就是表结构；取自状态明细同名列
LATEST_COLUMNS = [
    "icao24",
    "event_time",            # 最后出现：快照时间（UTC）
    "callsign",
    "origin_country",        # 注册国，不是出发地
    "on_ground",
    "squawk",
    "latitude",
    "longitude",
    "altitude_m",
    "speed_kmh",
    "vertical_rate",
    "flight_phase",
    "heading_deg",
    "heading_dir",
    "near_airport_iata",     # 最近机场；进近、离场按它归属
    "near_airport_name",
    "near_airport_city",
    "airport_dist_km",
    "kafka_ts",              # 这条记录写入 Kafka 的时间，算处理延迟用
    "departed",              # 已离开
]
STATE_INPUT = [c for c in LATEST_COLUMNS if c != "departed"]
LATEST_KEY = ("icao24",)


def departed_before(clock: str) -> str:
    """最后出现早于这个时刻（UTC 字符串）就算已离开。"""
    return f"TIMESTAMP '{clock}' - INTERVAL {DEPARTED_AFTER_S} SECONDS"


def newest_per_aircraft(states: DataFrame) -> DataFrame:
    """每架飞机时间最晚的一条。同一架飞机同一时刻只有一条（状态明细已按主键去重），结果是确定的。"""
    return (
        states.select(*STATE_INPUT)
        .groupBy("icao24")
        .agg(F.max_by(F.struct(*STATE_INPUT), "event_time").alias("_r"))
        .select("_r.*")
    )


def latest_updates(current: DataFrame, states: DataFrame, clock: str) -> DataFrame:
    """本批之后要写入 ads_latest_position 的行，列同 LATEST_COLUMNS。

      current  表里现有的最新状态（全表，几万行以内）；第一次建表时传空表
      states   本批的状态明细；全量重算时传整张状态明细
      clock    数据时钟（UTC 字符串）：已处理数据里最晚的快照时间，调用方保证它不往回走

    返回三类行：新出现的飞机、出现了更晚记录的飞机（位置更新，已离开随之清除）、
    没有新记录但刚超过 1 分钟的飞机（标记为已离开）。其余行不变，不返回。
    只取更晚的记录，所以同一批重放、或者晚到一条更早的记录，都不会把位置改回去。
    """
    _require_utc(states)
    new = newest_per_aircraft(states)
    new = new.select([F.col(c).alias(f"_n_{c}") for c in STATE_INPUT])
    cur = current.select(*LATEST_COLUMNS)
    joined = cur.join(new, cur["icao24"] == new["_n_icao24"], "full_outer")

    take_new = F.col("_n_icao24").isNotNull() & (
        F.col("icao24").isNull() | (F.col("_n_event_time") > F.col("event_time")))
    merged = joined.select(
        *[F.when(take_new, F.col(f"_n_{c}")).otherwise(F.col(c)).alias(c) for c in STATE_INPUT],
        take_new.alias("_take_new"),
        F.col("departed").alias("_was_departed"),
    )
    return (
        merged
        .withColumn("departed", F.expr(f"event_time < {departed_before(clock)}"))
        .where("_take_new OR departed <> _was_departed")
        .select(*LATEST_COLUMNS)
    )


def _require_utc(df: DataFrame) -> None:
    tz = df.sparkSession.conf.get("spark.sql.session.timeZone")
    if tz not in ("UTC", "Etc/UTC", "GMT", "Z", "+00:00"):
        raise ValueError(
            f"时间按 UTC 解释，Spark 会话时区必须是 UTC，现在是 {tz}。"
            "在 SparkSession 上设置 spark.sql.session.timeZone=UTC。"
        )
