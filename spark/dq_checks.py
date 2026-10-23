"""
数据质量门禁：按区间检查，结果写 dq_result
==========================================
在 DWS 之后、ADS 之前跑。强规则失败 → 退出码 1，Airflow 不会往下走，这一段的 ADS 不发布
（看板保留上一版）；弱规则只记录、只进日报。

10 条规则来自 docs/requirements.md「数据质量规则」，代码里的 code 和文档一一对应：

  snapshot_completeness   快照完整性：区间实收轮次 ÷ 应收轮次
  state_key_unique        状态主键唯一 (icao24, event_time)
  event_key_unique        事件主键唯一 (icao24, event_time, event_type)
  coords_in_bbox          坐标在采集范围内
  filter_rate_stable      过滤率稳定（ODS → DWD 的比例在基线 ±10 个百分点内）
  legs_closed             没有超过 20 分钟仍为 IN_AIR 的航段
  landing_leg_has_event   每个 LANDED 航段恰好对应一个落地事件
  alert_key_unique        告警表 MERGE 后主键无重复（等价于"行数等于本次计算行数"）
  stream_batch_reconcile  流批在飞架数相对差 ≤ 10%（可选流式表，没跑流式作业时跳过）

跑法（在 spark 容器里）：
  spark-submit jobs/dq_checks.py --start "2026-10-20 00:00" --end "2026-10-20 06:00"
  spark-submit jobs/dq_checks.py --start ... --end ... --fail-on-strong   # Airflow 用这个

★ 为什么"完整性"要按轮次算，不按行数
  一行是一架飞机，航班多少随时在变；一轮是一个快照，采集器每 10 秒一轮是固定的。
  收了多少轮是确定的，收了多少行要看当时天上几架飞机。
  分母按配置的轮询间隔算，改采集频率时用环境变量 DQ_POLL_INTERVAL 同步。

★ 为什么强规则只挡住"发布"，不删数据
  质量规则的作用是让下游别拿坏数据做决策，不是把已经落地的明细删掉。
  明细留着，修好之后重跑这一段就能把 ADS 补回来（见 runbook 的批链路一节）。
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, StringType, StructField, StructType, TimestampType,
)

BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/delta/bronze_flights")
DWD_STATE_PATH = os.getenv("DWD_STATE_PATH", "/data/delta/dwd_flight_state")
DWD_EVENT_PATH = os.getenv("DWD_EVENT_PATH", "/data/delta/dwd_airport_event")
DWD_LEG_PATH = os.getenv("DWD_LEG_PATH", "/data/delta/dwd_flight_leg")
GOLD_STREAM_PATH = os.getenv("GOLD_STREAM_PATH", "/data/delta/gold_overview_stream")
ALERT_PATH = os.getenv("ADS_ALERT_PATH", "/data/delta/ads/flight_alert")
GOLD_OVERVIEW_PATH = os.getenv("GOLD_OVERVIEW_PATH", "/data/delta/gold_overview_5min")
DQ_PATH = os.getenv("DQ_RESULT_PATH", "/data/delta/dq_result")

POLL_INTERVAL_S = int(os.getenv("DQ_POLL_INTERVAL", "10"))   # 采集轮询间隔（秒）
COMPLETENESS_STRONG = 0.90
COMPLETENESS_WEAK = 0.99
FILTER_RATE_BASELINE = float(os.getenv("DQ_FILTER_BASELINE", "0.0005"))
FILTER_RATE_TOLERANCE = 0.10
LEG_TIMEOUT_MIN = 20
RECONCILE_TOLERANCE = 0.10

# 结果除了写 Delta，还往 PostgreSQL 写一份：告警引擎和看板查服务层，不查湖
PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/flightdb")
PG_PROPS = {
    "user": os.getenv("PG_USER", "flight"),
    "password": os.getenv("PG_PASSWORD", "flight123"),
    "driver": "org.postgresql.Driver",
}

# 采集范围（和采集器的 BBOX_* 一致；改范围先改这里和 requirements.md）
BBOX = {
    "lat_min": float(os.getenv("BBOX_LAMIN", "43.0")),
    "lon_min": float(os.getenv("BBOX_LOMIN", "2.5")),
    "lat_max": float(os.getenv("BBOX_LAMAX", "52.0")),
    "lon_max": float(os.getenv("BBOX_LOMAX", "12.5")),
}

RESULT_SCHEMA = StructType([
    StructField("interval_start", TimestampType()),
    StructField("interval_end", TimestampType()),
    StructField("rule_code", StringType()),
    StructField("level", StringType()),
    StructField("passed", BooleanType()),
    StructField("metric", DoubleType()),
    StructField("threshold", DoubleType()),
    StructField("detail", StringType()),
    StructField("checked_at", TimestampType()),
    StructField("dt", DateType()),
])

# 每条规则在结果表里的中文名，打印时用
RULE_NAMES = {
    "snapshot_completeness": "快照完整性",
    "state_key_unique": "状态主键唯一",
    "event_key_unique": "事件主键唯一",
    "coords_in_bbox": "坐标在采集范围内",
    "filter_rate_stable": "过滤率稳定",
    "legs_closed": "航段已闭合",
    "landing_leg_has_event": "起降与航段对得上",
    "alert_key_unique": "告警表主键无重复",
    "stream_batch_reconcile": "流批在飞架数一致",
}

# 单条结果规则的级别（完整性规则自己返回强/弱两条，不在表里）
LEVELS = {
    "state_key_unique": "strong",
    "event_key_unique": "strong",
    "coords_in_bbox": "strong",
    "alert_key_unique": "strong",
    "filter_rate_stable": "weak",
    "legs_closed": "weak",
    "landing_leg_has_event": "weak",
    "stream_batch_reconcile": "weak",
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DQ-Checks")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _range(col: str, start: str, end: str) -> str:
    return f"{col} >= TIMESTAMP '{start}' AND {col} < TIMESTAMP '{end}'"


def _exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


# ================================================================
#  每条规则返回一个 (passed, metric, threshold, detail) 或 None（不适用）
# ================================================================
def check_completeness(spark, start, end):
    """区间实收轮次 ÷ 应收轮次。返回两条：强（90%）和弱（99%）。"""
    if not _exists(spark, BRONZE_PATH):
        return []
    rounds = (spark.read.format("delta").load(BRONZE_PATH)
              .where(_range("event_time", start, end))
              .select("event_time").distinct().count())
    seconds = (datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
               - datetime.strptime(start, "%Y-%m-%d %H:%M:%S")).total_seconds()
    expected = max(1, int(seconds // POLL_INTERVAL_S))
    ratio = rounds / expected
    detail = f"实收 {rounds} 轮 / 应收 {expected} 轮（每 {POLL_INTERVAL_S} 秒一轮）"
    return [
        ("strong", ratio >= COMPLETENESS_STRONG, ratio, COMPLETENESS_STRONG, detail),
        ("weak", ratio >= COMPLETENESS_WEAK, ratio, COMPLETENESS_WEAK, detail),
    ]


def check_state_key_unique(spark, start, end):
    if not _exists(spark, DWD_STATE_PATH):
        return None
    dups = (spark.read.format("delta").load(DWD_STATE_PATH)
            .where(_range("event_time", start, end))
            .groupBy("icao24", "event_time").count().where("count > 1").count())
    return dups == 0, float(dups), 0.0, f"重复主键 {dups} 组"


def check_event_key_unique(spark, start, end):
    if not _exists(spark, DWD_EVENT_PATH):
        return None
    dups = (spark.read.format("delta").load(DWD_EVENT_PATH)
            .where(_range("event_time", start, end))
            .groupBy("icao24", "event_time", "event_type").count()
            .where("count > 1").count())
    return dups == 0, float(dups), 0.0, f"重复主键 {dups} 组"


def check_coords_in_bbox(spark, start, end):
    if not _exists(spark, DWD_STATE_PATH):
        return None
    states = (spark.read.format("delta").load(DWD_STATE_PATH)
              .where(_range("event_time", start, end)))
    outside = states.where(
        f"latitude < {BBOX['lat_min']} OR latitude > {BBOX['lat_max']} "
        f"OR longitude < {BBOX['lon_min']} OR longitude > {BBOX['lon_max']}"
    ).count()
    return outside == 0, float(outside), 0.0, f"范围外坐标 {outside} 条"


def check_filter_rate_stable(spark, start, end):
    if not (_exists(spark, BRONZE_PATH) and _exists(spark, DWD_STATE_PATH)):
        return None
    ods = (spark.read.format("delta").load(BRONZE_PATH)
           .where(_range("event_time", start, end)).count())
    if ods == 0:
        return None
    dwd = (spark.read.format("delta").load(DWD_STATE_PATH)
           .where(_range("event_time", start, end)).count())
    rate = max(0.0, 1.0 - dwd / ods)
    return (abs(rate - FILTER_RATE_BASELINE) <= FILTER_RATE_TOLERANCE, rate,
            FILTER_RATE_BASELINE,
            f"ODS {ods} 行 → DWD {dwd} 行，过滤率 {rate:.2%}")


def check_legs_closed(spark, start, end):
    if not (_exists(spark, DWD_LEG_PATH) and _exists(spark, DWD_STATE_PATH)):
        return None
    clock = (spark.read.format("delta").load(DWD_STATE_PATH)
             .agg(F.max("event_time")).first()[0])
    if clock is None:
        return None
    cutoff = clock - timedelta(minutes=LEG_TIMEOUT_MIN)
    stale = (spark.read.format("delta").load(DWD_LEG_PATH)
             .where("status = 'IN_AIR'")
             .where(f"last_seen < TIMESTAMP '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'")
             .count())
    return stale == 0, float(stale), 0.0, f"超时仍为 IN_AIR 的航段 {stale} 个"


def check_landing_leg_has_event(spark, start, end):
    if not (_exists(spark, DWD_LEG_PATH) and _exists(spark, DWD_EVENT_PATH)):
        return None
    legs = (spark.read.format("delta").load(DWD_LEG_PATH)
            .where("status = 'LANDED'")
            .where(_range("end_time", start, end))
            .select("icao24", "leg_id", "end_time"))
    events = (spark.read.format("delta").load(DWD_EVENT_PATH)
              .where("event_type = 'LANDING'")
              .where(_range("event_time", start, end))
              .select("icao24", F.col("event_time").alias("end_time")))
    matched = legs.join(events, ["icao24", "end_time"], "left_anti")
    n_legs, n_missing = legs.count(), matched.count()
    return (n_missing == 0, float(n_missing), 0.0,
            f"{n_legs} 个落地航段里 {n_missing} 个没有对应的落地事件")


def check_alert_key_unique(spark, start, end):
    if not _exists(spark, ALERT_PATH):
        return None
    dups = (spark.read.format("delta").load(ALERT_PATH)
            .where(_range("event_time", start, end))
            .groupBy("icao24", "event_time", "alert_type").count()
            .where("count > 1").count())
    return dups == 0, float(dups), 0.0, f"重复告警主键 {dups} 组"


def check_stream_batch_reconcile(spark, start, end):
    """流式窗口总览 vs 批式窗口总览。流式作业没跑时这条不适用。"""
    if not _exists(spark, GOLD_STREAM_PATH):
        return None
    stream_all = spark.read.format("delta").load(GOLD_STREAM_PATH)
    if "aircraft_cnt" not in stream_all.columns:
        # 流式表还是 M3 之前的列名（total_flights），没有对得上的字段，跳过
        return None
    stream = (stream_all.where(_range("window_start", start, end))
              .select("window_start", F.col("aircraft_cnt").alias("stream_cnt")))
    if stream.limit(1).count() == 0:
        return None
    batch = (spark.read.format("delta").load(GOLD_OVERVIEW_PATH)
             .where(_range("window_start", start, end))
             .select("window_start", F.col("aircraft_cnt").alias("batch_cnt")))
    joined = stream.join(batch, "window_start")
    stats = joined.select(
        F.count(F.lit(1)).alias("n"),
        F.sum(F.when(
            F.col("batch_cnt") > 0,
            F.abs(F.col("stream_cnt") - F.col("batch_cnt")) / F.col("batch_cnt")
        )).alias("rel_diff_sum"),
        F.max(F.when(
            F.col("batch_cnt") > 0,
            F.abs(F.col("stream_cnt") - F.col("batch_cnt")) / F.col("batch_cnt")
        )).alias("rel_diff_max"),
    ).first()
    if not stats or not stats["n"]:
        return None
    # 用窗口最大相对差判断：窗口很多时平均值会把个别坏窗口抹平
    worst = float(stats["rel_diff_max"] or 0.0)
    return (worst <= RECONCILE_TOLERANCE, worst, RECONCILE_TOLERANCE,
            f"{stats['n']} 个窗口里最大相对差 {worst:.1%}（流式 HLL 约 5% 误差）")


CHECKS = {
    "snapshot_completeness": check_completeness,
    "state_key_unique": check_state_key_unique,
    "event_key_unique": check_event_key_unique,
    "coords_in_bbox": check_coords_in_bbox,
    "filter_rate_stable": check_filter_rate_stable,
    "legs_closed": check_legs_closed,
    "landing_leg_has_event": check_landing_leg_has_event,
    "alert_key_unique": check_alert_key_unique,
    "stream_batch_reconcile": check_stream_batch_reconcile,
}


# ================================================================
#  执行与写结果
# ================================================================
def run_checks(spark: SparkSession, start: str, end: str) -> DataFrame:
    rows = []
    for code, fn in CHECKS.items():
        result = fn(spark, start, end)
        if result is None:
            continue
        # 完整性规则自己返回两条（强/弱两个阈值），其余规则返回一条，级别查这张表
        items = result if isinstance(result, list) else [(LEVELS[code], *result)]
        for level, passed, metric, threshold, detail in items:
            rows.append((start, end, code, level, bool(passed), metric, threshold, detail))

    # 汇总一行"要不要发布"：ADS 只看这一行，不用自己去判断哪条算强规则。
    # level 用 gate 而不是 strong，免得告警引擎把它当成一条失败的强规则再报一次。
    strong_failed = sum(1 for r in rows if r[3] == "strong" and not r[4])
    rows.append((start, end, "ads_gate", "gate", strong_failed == 0,
                 float(strong_failed), 0.0,
                 "强规则全部通过，可以发布 ADS" if strong_failed == 0
                 else f"{strong_failed} 条强规则未通过，这一段 ADS 不发布"))

    schema = RESULT_SCHEMA
    # 一次检查的所有行共用一个时间戳：看板按 max(checked_at) 取"最近一次检查"，
    # 每行各取一次 now() 的话（微秒级不同）只会取到最后一行 ads_gate。
    checked_at = datetime.utcnow()
    df = spark.createDataFrame(
        [(datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S"),
          datetime.strptime(r[1], "%Y-%m-%d %H:%M:%S"),
          r[2], r[3], r[4], float(r[5]), float(r[6]), r[7],
          checked_at, datetime.strptime(r[0][:10], "%Y-%m-%d"))
         for r in rows],
        schema,
    )
    return df


def write_results(spark: SparkSession, df: DataFrame, start: str, end: str) -> None:
    """写 dq_result：同一区间重跑时覆盖这个区间，不同区间互不影响。"""
    if not DeltaTable.isDeltaTable(spark, DQ_PATH):
        (df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").partitionBy("dt").save(DQ_PATH))
        return
    cond = (f"dt BETWEEN DATE '{start[:10]}' AND DATE '{end[:10]}' "
            f"AND interval_start = TIMESTAMP '{start}' AND interval_end = TIMESTAMP '{end}'")
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", cond).save(DQ_PATH))


def write_to_pg(df: DataFrame) -> None:
    """往 PostgreSQL 写一份，服务层的告警引擎和看板直接查，不用读 Delta。

    ★ truncate 而不是 DROP 重建：dq_result 每 15 分钟重写一次，DROP 之后表的 oid 变了，
      grafana_ro 的 SELECT 权限随之丢失，统计看板的质量面板会一直报 permission denied，
      直到下次重启实时作业才补回来（项目报告 M3）。
    """
    (
        df.write.format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", "dq_result")
        .option("user", PG_PROPS["user"])
        .option("password", PG_PROPS["password"])
        .option("driver", PG_PROPS["driver"])
        .option("batchsize", "100")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )
    print("[dq] → PostgreSQL.dq_result 写入完成")


def summarize(df: DataFrame) -> tuple[int, int]:
    strong_failed = df.where("level = 'strong' AND NOT passed").count()
    weak_failed = df.where("level = 'weak' AND NOT passed").count()
    for row in df.orderBy("rule_code", "level").collect():
        mark = "✓" if row["passed"] else "✗"
        name = RULE_NAMES.get(row["rule_code"], row["rule_code"])
        print(f"[dq] {mark} {row['rule_code']:<24} {name:<12} {row['level']:<6} "
              f"{row['detail']}")
    return strong_failed, weak_failed


def parse_utc(text: str) -> str:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"时间格式不对：{text}")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="数据质量门禁：按区间检查并写 dq_result")
    p.add_argument("--start", type=parse_utc, required=True, help="区间开始，UTC")
    p.add_argument("--end", type=parse_utc, required=True, help="区间结束，UTC")
    p.add_argument("--fail-on-strong", action="store_true",
                   help="强规则失败时退出码 1（Airflow 用；手动排查时不要加）")
    return p.parse_args(argv)


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    started = time.time()

    print("=" * 66)
    print(f"  数据质量门禁  [{args.start}, {args.end}) UTC")
    print("=" * 66)
    results = run_checks(spark, args.start, args.end)
    write_results(spark, results, args.start, args.end)
    try:
        write_to_pg(results)
    except Exception as e:  # noqa: BLE001 —— PG 不可用不能让质量检查本身失败
        print(f"[dq] 写 PostgreSQL 失败（湖里的结果已经写了）：{e!r}")
    strong_failed, weak_failed = summarize(results)
    print(f"[dq] 强规则失败 {strong_failed} 条，弱规则失败 {weak_failed} 条，"
          f"用时 {time.time() - started:.1f} 秒")

    spark.stop()
    if strong_failed and args.fail_on_strong:
        sys.exit(1)


if __name__ == "__main__":
    main()
