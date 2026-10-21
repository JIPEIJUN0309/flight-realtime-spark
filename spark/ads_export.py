"""
ADS（服务层）：DWS → 应用表 → PostgreSQL
==========================================
读 DWS 汇总表，算成看板直接查的 ADS 表，Delta 里 MERGE 一份，再整表写进 PostgreSQL。

产出 6 张表：

  ads_airport_traffic   各机场 5 分钟流量排名（架数、进近、离场）
  ads_airport_hourly    各机场 UTC 小时起降架次（M4 新增，Q2 用）
  ads_country_share     各国飞机数占比
  ads_realtime_kpi      全区域 5 分钟总览 KPI
  ads_flight_alert      规则告警（阈值见下）
  （最新位置 ads_latest_position 由 rt_pipeline.py 实时写，这里不碰
    —— 整表覆盖会把它连同主键一起删掉重建）

★ 为什么 MERGE 的删除只限重算区间
  告警和聚合都是"按区间重算"的：这次算 12:00–12:15，就只应该动这 15 分钟的行。
  如果像 M3 之前那样 whenNotMatchedBySourceDelete() 不带条件，目标表里"源里没有"的行
  全会被删掉 —— 源只有 15 分钟的数据，结果是把历史全清空。
  所以删除条件带上区间：区间内按源对齐（源里没有就删，规则改了旧的误报才会消失），
  区间外一行不动。这就是需求文档里"MERGE 同步，删除只限重算区间"。

★ 区间从哪来
  不传 --start/--end 时，按 DWS 表里最新的窗口往前看一段（5 分钟表 1 小时、小时表 6 小时）。
  DWS 是上游，它算到哪，ADS 就跟到哪；往回多看一段是为了覆盖迟到数据和上次没写完的尾巴。
  传了区间就按区间重算，补数时用。

★ 表结构变了
  M4 把 flight_cnt / total_flights 改名成 aircraft_cnt，表结构对不上。
  Delta 的 MERGE 要求两边列一致，遇到这种情况自动整表重建（ADS 全是算出来的派生数据）。
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

# ---------------- 路径与 PostgreSQL 配置 ----------------
DWS_AIRPORT_PATH = os.getenv("DWS_AIRPORT_PATH", "/data/delta/dws_airport_5min")
DWS_HOURLY_PATH = os.getenv("DWS_HOURLY_PATH", "/data/delta/dws_airport_hourly")
GOLD_COUNTRY_PATH = os.getenv("GOLD_COUNTRY_PATH", "/data/delta/gold_country_5min")
GOLD_OVERVIEW_PATH = os.getenv("GOLD_OVERVIEW_PATH", "/data/delta/gold_overview_5min")
DWD_STATE_PATH = os.getenv("DWD_STATE_PATH", "/data/delta/dwd_flight_state")
DQ_PATH = os.getenv("DQ_RESULT_PATH", "/data/delta/dq_result")
# 质量检查结果的有效期：超过这么久没检查过，就不再拿它挡发布
DQ_MAX_AGE_MIN = int(os.getenv("ADS_DQ_MAX_AGE_MIN", "120"))

ADS_BASE = os.getenv("ADS_BASE", "/data/delta/ads")

PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/flightdb")
PG_PROPS = {
    "user": os.getenv("PG_USER", "flight"),
    "password": os.getenv("PG_PASSWORD", "flight123"),
    "driver": "org.postgresql.Driver",
}

TS_FMT = "yyyy-MM-dd HH:mm:ss"
LOOKBACK_MINUTES = int(os.getenv("ADS_LOOKBACK_MINUTES", "60"))
LOOKBACK_HOURS = int(os.getenv("ADS_LOOKBACK_HOURS", "6"))

# ================================================================
#  规则告警阈值（和 M3 之前一致；校准过程见 README「异常检测」）
# ================================================================
#  15 m/s = 2,953 ft/min，是客机正常的初始爬升，不是异常。第一版用它报了 1.9% 的
#  "急剧爬升"，等于在告警"飞机起飞了"。改成 25 m/s（≈4,900 ft/min）后只剩个位数。
TH_RAPID_CLIMB = 25          # m/s
TH_RAPID_DESCENT = -30       # m/s
TH_HIGH_ALT = 14500          # m
TH_HIGH_SPEED = 1100         # km/h
TH_LOW_ALT = 1500            # m
TH_LOW_ALT_SPEED = 500       # km/h
TH_FAR_AIRPORT = 50          # km

# ================================================================
#  每张 ADS 表的列（也是表结构）
# ================================================================
TRAFFIC_COLUMNS = [
    "window_start", "airport_iata", "airport_city",
    "aircraft_cnt", "arriving_cnt", "departing_cnt",
    "avg_altitude_m", "avg_speed_kmh", "rank_in_window", "dt",
]
HOURLY_COLUMNS = [
    "hour_start", "hour_end", "airport_iata", "airport_city",
    "takeoff_cnt", "landing_cnt", "movement_cnt", "rank_in_window", "dt",
]
COUNTRY_COLUMNS = [
    "window_start", "origin_country", "aircraft_cnt",
    "avg_altitude_m", "avg_speed_kmh", "pct_of_window", "rank_in_window", "dt",
]
KPI_COLUMNS = [
    "window_start", "window_end", "aircraft_cnt", "country_cnt",
    "avg_altitude_m", "avg_speed_kmh", "max_speed_kmh", "max_altitude_m",
    "climbing_cnt", "descending_cnt", "cruising_cnt", "near_airport_cnt",
    "takeoff_landing_pct", "dt",
]
ALERT_COLUMNS = [
    "icao24", "callsign", "origin_country", "latitude", "longitude",
    "altitude_m", "speed_kmh", "vertical_rate", "near_airport_city",
    "airport_dist_km", "event_time", "alert_type", "alert_detail", "dt",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ADS-Export")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ================================================================
#  指标转换（纯计算，不读表）
# ================================================================
def build_traffic(dws: DataFrame) -> DataFrame:
    """机场 5 分钟流量 + 窗口内排名。"""
    return (
        dws
        .withColumn("rank_in_window",
                    F.rank().over(Window.partitionBy("window_start")
                                  .orderBy(F.col("aircraft_cnt").desc())))
        .select("window_start", "airport_iata", "airport_city",
                "aircraft_cnt", "arriving_cnt", "departing_cnt",
                "avg_altitude_m", "avg_speed_kmh", "rank_in_window", "dt")
    )


def build_hourly(dws: DataFrame) -> DataFrame:
    """机场小时起降 + 同一小时内按架次排名。"""
    return (
        dws
        .withColumn("rank_in_window",
                    F.rank().over(Window.partitionBy("hour_start")
                                  .orderBy(F.col("movement_cnt").desc())))
        .select("hour_start", "hour_end", "airport_iata", "airport_city",
                "takeoff_cnt", "landing_cnt", "movement_cnt",
                "rank_in_window", "dt")
    )


def build_country(dws: DataFrame) -> DataFrame:
    return dws.select("window_start", "origin_country", "aircraft_cnt",
                      "avg_altitude_m", "avg_speed_kmh", "pct_of_window",
                      "rank_in_window", "dt")


def build_kpi(dws: DataFrame) -> DataFrame:
    """总览 KPI，外加一个派生指标：起降活跃度（爬升+下降占在飞架数）。"""
    pct = F.when(F.col("aircraft_cnt") > 0,
                 F.round(100.0 * (F.col("climbing_cnt") + F.col("descending_cnt"))
                         / F.col("aircraft_cnt"), 1))
    return (
        dws
        .withColumn("takeoff_landing_pct", pct)
        .select("window_start", "window_end", "aircraft_cnt", "country_cnt",
                "avg_altitude_m", "avg_speed_kmh", "max_speed_kmh", "max_altitude_m",
                "climbing_cnt", "descending_cnt", "cruising_cnt", "near_airport_cnt",
                "takeoff_landing_pct", "dt")
    )


ALERT_SQL = f"""
WITH tagged AS (
    SELECT
        icao24, callsign, origin_country,
        latitude, longitude, altitude_m, speed_kmh, vertical_rate,
        near_airport_city, airport_dist_km, event_time,
        CASE
            WHEN vertical_rate > {TH_RAPID_CLIMB}   THEN 'RAPID_CLIMB'
            WHEN vertical_rate < {TH_RAPID_DESCENT} THEN 'RAPID_DESCENT'
            WHEN altitude_m    > {TH_HIGH_ALT}      THEN 'UNUSUAL_ALTITUDE'
            WHEN speed_kmh     > {TH_HIGH_SPEED}    THEN 'UNUSUAL_SPEED'
            WHEN altitude_m    < {TH_LOW_ALT}
                 AND speed_kmh > {TH_LOW_ALT_SPEED}
                 AND airport_dist_km > {TH_FAR_AIRPORT}
                                                    THEN 'LOW_ALT_FAR_FROM_AIRPORT'
            ELSE NULL
        END AS alert_type,
        CASE
            WHEN vertical_rate > {TH_RAPID_CLIMB} THEN
                CONCAT('急剧爬升 ', ROUND(vertical_rate, 1), ' m/s')
            WHEN vertical_rate < {TH_RAPID_DESCENT} THEN
                CONCAT('急剧下降 ', ROUND(vertical_rate, 1), ' m/s')
            WHEN altitude_m > {TH_HIGH_ALT} THEN
                CONCAT('异常高度 ', CAST(ROUND(altitude_m, 0) AS INT), ' m')
            WHEN speed_kmh > {TH_HIGH_SPEED} THEN
                CONCAT('异常速度 ', ROUND(speed_kmh, 1), ' km/h')
            WHEN altitude_m < {TH_LOW_ALT}
                 AND speed_kmh > {TH_LOW_ALT_SPEED}
                 AND airport_dist_km > {TH_FAR_AIRPORT} THEN
                CONCAT('低空高速且远离机场 ',
                       CAST(ROUND(airport_dist_km, 0) AS INT), ' km')
            ELSE NULL
        END AS alert_detail
    FROM dwd_flight_state
),
deduped AS (
    -- MERGE 的源对同一目标行只能有一条匹配，否则 Delta 直接报错。
    -- 状态明细按主键去重过，这里再按 MERGE 键兜一次底。
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY icao24, event_time, alert_type
            ORDER BY event_time
        ) AS rn
    FROM tagged
    WHERE alert_type IS NOT NULL
)
SELECT
    icao24, callsign, origin_country,
    latitude, longitude,
    altitude_m, speed_kmh, vertical_rate,
    near_airport_city, airport_dist_km,
    event_time, alert_type, alert_detail,
    DATE(event_time) AS dt
FROM deduped
WHERE rn = 1
"""


def build_alerts(spark: SparkSession, start: str, end: str) -> DataFrame:
    """规则告警：从状态明细的空中记录里找。地面记录不需要看这些阈值。"""
    spark.read.format("delta").load(DWD_STATE_PATH) \
        .where(f"event_time >= TIMESTAMP '{start}' AND event_time < TIMESTAMP '{end}'") \
        .where("NOT on_ground") \
        .createOrReplaceTempView("dwd_flight_state")
    return spark.sql(ALERT_SQL)


# ================================================================
#  写 Delta：MERGE + 区间内同步删除
# ================================================================
def _last_day(end: str) -> str:
    return (datetime.strptime(end, "%Y-%m-%d %H:%M:%S") - timedelta(seconds=1)) \
        .strftime("%Y-%m-%d")


def write_ads(spark: SparkSession, df: DataFrame, name: str, columns: list,
              keys: list, col: str, start: str, end: str, rebuild: bool = False) -> int:
    path = f"{ADS_BASE}/{name}"
    rows = df.count()

    if not DeltaTable.isDeltaTable(spark, path):
        (df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").partitionBy("dt").save(path))
        print(f"[ads] {name}: 首次建表，{rows} 行")
        return rows

    existing = set(spark.read.format("delta").load(path).columns)
    if existing != set(columns):
        if not rebuild:
            # 正常路径上 run() 已经先检查过并扩成整表重建了。走到这里说明是
            # 带 --start/--end 的补数撞上了表结构变化：这时按区间覆盖会把区间外的历史冲掉，
            # 所以直接停下，让人先跑一次不带区间的整表重建。
            raise RuntimeError(
                f"[ads] {name} 的表结构和代码不一致，不能按区间 MERGE。"
                "先不带 --start/--end 跑一次 ads_export.py 让整表重建，再补数。")
        print(f"[ads] {name}: 表结构变了（{len(existing)} 列 → {len(columns)} 列），整表重建")
        (df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").partitionBy("dt").save(path))
        return rows

    cond = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    in_interval = (f"t.dt BETWEEN DATE '{start[:10]}' AND DATE '{_last_day(end)}' "
                   f"AND t.{col} >= TIMESTAMP '{start}' AND t.{col} < TIMESTAMP '{end}'")
    (
        DeltaTable.forPath(spark, path).alias("t")
        .merge(df.alias("s"), cond)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete(in_interval)
        .execute()
    )
    print(f"[ads] {name}: MERGE [{start}, {end}) 完成，源 {rows} 行")
    return rows


# ================================================================
#  PostgreSQL：整表覆盖
# ================================================================
def write_to_pg(df: DataFrame, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .option("user", PG_PROPS["user"])
        .option("password", PG_PROPS["password"])
        .option("driver", PG_PROPS["driver"])
        .option("batchsize", "1000")
        # ★ 用 truncate 而不是 drop 重建：
        #   Spark 的 overwrite 默认是 DROP TABLE + CREATE，表被删掉重建后，Metabase 里
        #   按字段 ID 保存的卡片全部失效（每次批任务都会把看板弄坏一次）。truncate 只清空
        #   行、保留表和列，Metabase 的字段元数据不会失效。
        #   代价：表结构必须和 DataFrame 一致；改列名之后要先手工 DROP 一次让 Spark 重建。
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print(f"[ads] → PostgreSQL.{table} 写入完成")


def sync_pg(spark: SparkSession, name: str, table: str | None = None) -> None:
    write_to_pg(spark.read.format("delta").load(f"{ADS_BASE}/{name}"), table or f"ads_{name}")


# ================================================================
#  区间
# ================================================================
def _snap(text: str, minutes: int) -> str:
    dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(minute=(dt.minute // minutes) * minutes, second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _shift(text: str, **kwargs) -> str:
    return (datetime.strptime(text, "%Y-%m-%d %H:%M:%S") + timedelta(**kwargs)) \
        .strftime("%Y-%m-%d %H:%M:%S")


def _latest(spark: SparkSession, path: str, col: str) -> str | None:
    if not DeltaTable.isDeltaTable(spark, path):
        sys.exit(f"[ads] 找不到 {path}，先跑 dws_batch.py 生成 DWS 表。")
    row = (spark.read.format("delta").load(path)
           .agg(F.date_format(F.max(col), TS_FMT).alias("t")).first())
    return row["t"] if row and row["t"] else None


def resolve_5min(spark: SparkSession, start: str | None, end: str | None) -> tuple[str, str]:
    if start and end:
        return _snap(start, 5), _snap(end, 5)
    latest = _latest(spark, DWS_AIRPORT_PATH, "window_start")
    if not latest:
        sys.exit("[ads] dws_airport_5min 是空的，先跑 dws_batch.py。")
    end_s = _shift(_snap(latest, 5), minutes=5)
    return _shift(end_s, minutes=-LOOKBACK_MINUTES), end_s


def resolve_hourly(spark: SparkSession, start: str | None, end: str | None) -> tuple[str, str]:
    if start and end:
        return _snap(start, 60), _snap(end, 60)
    latest = _latest(spark, DWS_HOURLY_PATH, "hour_start")
    if not latest:
        sys.exit("[ads] dws_airport_hourly 是空的，先跑 dws_batch.py。")
    end_s = _shift(_snap(latest, 60), hours=1)
    return _shift(end_s, hours=-LOOKBACK_HOURS), end_s


def _range(df: DataFrame, col: str, start: str, end: str) -> DataFrame:
    return df.where(f"{col} >= TIMESTAMP '{start}' AND {col} < TIMESTAMP '{end}'")


# ================================================================
#  质量门禁：最近一次 dq_checks 说不能发布，就不发布
# ================================================================
def dq_gate(spark: SparkSession) -> tuple[bool, str]:
    """读 dq_checks 写的 ads_gate 行。返回 (能不能发布, 说明)。

    Airflow 里 dq_checks 在 ads_export 之前，强规则失败时 DAG 根本不会走到这里；
    这个检查是给手动跑和不经过 DAG 的场景兜底的。
    """
    if not DeltaTable.isDeltaTable(spark, DQ_PATH):
        return True, "还没有质量检查结果"
    row = (
        spark.read.format("delta").load(DQ_PATH)
        .where("rule_code = 'ads_gate'")
        .orderBy(F.col("checked_at").desc())
        .limit(1)
        .select("passed", "detail", "interval_start", "interval_end",
                F.expr("timestampdiff(MINUTE, checked_at, current_timestamp())").alias("age_min"))
        .first()
    )
    if row is None:
        return True, "还没有 ads_gate 记录"
    if row["age_min"] is not None and row["age_min"] > DQ_MAX_AGE_MIN:
        return True, f"最近一次质量检查是 {row['age_min']} 分钟前，已过期"
    if row["passed"]:
        return True, "质量强规则全部通过"
    return False, (f"{row['detail']}（区间 {row['interval_start']} → {row['interval_end']} UTC）")


# 每张 ADS 表的名字和列，用来判断表结构是否和代码一致
ADS_TABLES = {
    "airport_traffic": TRAFFIC_COLUMNS,
    "airport_hourly": HOURLY_COLUMNS,
    "country_share": COUNTRY_COLUMNS,
    "realtime_kpi": KPI_COLUMNS,
    "flight_alert": ALERT_COLUMNS,
}


def _needs_rebuild(spark: SparkSession) -> bool:
    """有表的列和代码对不上（M4 改名就是这种情况）。派生表，整表重建即可。"""
    for name, columns in ADS_TABLES.items():
        path = f"{ADS_BASE}/{name}"
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        if set(spark.read.format("delta").load(path).columns) != set(columns):
            return True
    return False


def _first(spark: SparkSession, path: str, col: str) -> str:
    row = (spark.read.format("delta").load(path)
           .agg(F.date_format(F.min(col), TS_FMT).alias("t")).first())
    return row["t"] if row and row["t"] else "1970-01-01 00:00:00"


# ================================================================
#  主流程
# ================================================================
def run(spark: SparkSession, start: str | None, end: str | None,
        ignore_dq: bool = False) -> None:
    started = time.time()
    print("=" * 66)
    print("  ADS 服务层：DWS → Delta MERGE → PostgreSQL")
    print(f"  告警阈值: 爬升>{TH_RAPID_CLIMB} m/s | 下降<{TH_RAPID_DESCENT} m/s "
          f"| 高度>{TH_HIGH_ALT} m")
    print("=" * 66)

    publish, why = dq_gate(spark)
    if not publish and not ignore_dq:
        print(f"[ads] 质量门禁未通过，这一段不发布：{why}")
        print("[ads] 看板继续显示上一版；修好数据后重跑 dq_checks.py 再跑本作业。")
        return
    print(f"[ads] 质量门禁：{why}{'（已忽略）' if not publish and ignore_dq else ''}")

    s5, e5 = resolve_5min(spark, start, end)
    sh, eh = resolve_hourly(spark, start, end)
    rebuild = bool(not start and _needs_rebuild(spark))
    if rebuild:
        # 表结构变了就不能按区间 MERGE，否则只写回这一段、把历史冲掉。
        # ADS 全是派生数据，从 DWS 的第一行重算到最新一行。
        s5, e5 = _first(spark, DWS_AIRPORT_PATH, "window_start"), e5
        sh, eh = _first(spark, DWS_HOURLY_PATH, "hour_start"), eh
        print("[ads] ADS 表结构有变，这次整表重建")
    print(f"[ads] 5 分钟表区间 [{s5}, {e5})，小时表区间 [{sh}, {eh}) UTC")

    airport = spark.read.format("delta").load(DWS_AIRPORT_PATH)
    hourly = spark.read.format("delta").load(DWS_HOURLY_PATH)
    country = spark.read.format("delta").load(GOLD_COUNTRY_PATH)
    overview = spark.read.format("delta").load(GOLD_OVERVIEW_PATH)

    traffic = build_traffic(_range(airport, "window_start", s5, e5))
    write_ads(spark, traffic, "airport_traffic", TRAFFIC_COLUMNS,
              ["window_start", "airport_iata"], "window_start", s5, e5, rebuild)

    hourly_ads = build_hourly(_range(hourly, "hour_start", sh, eh))
    write_ads(spark, hourly_ads, "airport_hourly", HOURLY_COLUMNS,
              ["hour_start", "airport_iata"], "hour_start", sh, eh, rebuild)

    share = build_country(_range(country, "window_start", s5, e5))
    write_ads(spark, share, "country_share", COUNTRY_COLUMNS,
              ["window_start", "origin_country"], "window_start", s5, e5, rebuild)

    kpi = build_kpi(_range(overview, "window_start", s5, e5))
    write_ads(spark, kpi, "realtime_kpi", KPI_COLUMNS,
              ["window_start"], "window_start", s5, e5, rebuild)

    alerts = build_alerts(spark, s5, e5)
    alert_rows = write_ads(spark, alerts, "flight_alert", ALERT_COLUMNS,
                           ["icao24", "event_time", "alert_type"], "event_time", s5, e5, rebuild)

    # ---- 写 PostgreSQL：看板只查这里 ----
    for name in ("airport_traffic", "airport_hourly", "country_share",
                 "realtime_kpi", "flight_alert"):
        sync_pg(spark, name)

    # ---- 自检：告警表 sync 之后行数应该和源一致（整表） ----
    table_cnt = spark.read.format("delta").load(f"{ADS_BASE}/flight_alert").count()
    print(f"[ads] 告警表整表 {table_cnt} 条，本次区间算出 {alert_rows} 条")
    if alert_rows and table_cnt == 0:
        print("[ads] ⚠️  告警表为空但本次算出了告警，检查 MERGE 的删除条件")

    print(f"[ads] 完成，用时 {time.time() - started:.1f} 秒")


def parse_utc(text: str) -> str:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"时间格式不对：{text}")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ADS：DWS → Delta MERGE → PostgreSQL")
    p.add_argument("--start", type=parse_utc, help="区间开始，UTC，包含")
    p.add_argument("--end", type=parse_utc, help="区间结束，UTC，不包含")
    p.add_argument("--ignore-dq", action="store_true",
                   help="即使最近一次质量检查没通过也发布（只在确认数据没问题时用）")
    args = p.parse_args(argv)
    if bool(args.start) != bool(args.end):
        p.error("--start 和 --end 要一起给")
    return args


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    run(spark, args.start, args.end, args.ignore_dq)
    spark.stop()


if __name__ == "__main__":
    main()
