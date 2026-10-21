"""
DWS 批处理：状态明细 / 起降事件 → 4 张汇总表
=============================================
读 dwd_flight_state（只取空中记录）和 dwd_airport_event，算：

  dws_airport_5min    各机场 5 分钟流量与进近/离场架数
  dws_airport_hourly  各机场 UTC 小时起降架次（来源是起降事件，不是状态明细）
  gold_country_5min   各国 5 分钟飞机数、占比、排名
  gold_overview_5min  全区域 5 分钟总览

三种模式（时间按 UTC 解释，区间左闭右开）：

  run        默认。只算"上次算到哪"之后的窗口，Airflow 每 15 分钟跑一次用这个
  backfill   指定 --start --end，重算这段区间；和 run 走同一段代码，只是区间由人指定
  verify     同一区间：表里的结果 vs 现在重算的结果，两个方向 EXCEPT ALL 都必须是 0

跑法（在 spark 容器里）：
  spark-submit jobs/dws_batch.py
  spark-submit jobs/dws_batch.py backfill --start "2026-10-20 00:00" --end "2026-10-20 06:00"
  spark-submit jobs/dws_batch.py verify   --start "2026-10-20 00:00" --end "2026-10-20 06:00"

★ 为什么是"按窗口边界对齐 + replaceWhere"，不是追加
  批任务每 15 分钟跑一次，但窗口是 5 分钟：上一次跑到 12:15，这一次算 12:15 之后。
  如果直接追加，12:15 那一刻可能还有迟到数据，或者上一次恰好在一个窗口中间停下，
  追加就会写出两个不完整的同一个窗口。
  这里的做法是：区间先向下对齐到窗口边界，再用 replaceWhere 覆盖同一区间。
  同一区间重算多少次，表里的行都一样 —— 这就是验收里的"批任务幂等"。
  默认还会往回多看一段（5 分钟表 1 小时、小时表 6 小时），把迟到数据和上一次没算完的
  尾巴一起覆盖掉。区间外的历史数据一个文件都不动。

★ 为什么 DWS 只算空中记录
  地面上的飞机不是"在飞"。状态明细保留了地面记录（起降判定要用），但流量指标只数空中的：
  aircraft_cnt 是"这一刻有多少架飞机在飞"，不是"机场里停了多少架"。

★ 表结构变了怎么办
  代码给表加了列、改了列名（比如 M4 把 flight_cnt 改成 aircraft_cnt），按区间覆盖会
  报 schema 不一致。这里的处理是自动整表重建：DWS 全部是从上游算出来的派生数据，
  没有一列是手工填的，重建不会丢东西。日志里会明确打印"表结构变了，整表重建"。
"""

import argparse
import os
import sys
import time
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

import dws_metrics as metrics
from dws_metrics import floor_hours, floor_minutes

# ---------------- 路径（环境变量可覆盖，本地测试时用）----------------
DWD_STATE_PATH = os.getenv("DWD_STATE_PATH", "/data/delta/dwd_flight_state")
DWD_EVENT_PATH = os.getenv("DWD_EVENT_PATH", "/data/delta/dwd_airport_event")

TABLE_PATHS = {
    "airport_5min": os.getenv("DWS_AIRPORT_PATH", "/data/delta/dws_airport_5min"),
    "airport_hourly": os.getenv("DWS_HOURLY_PATH", "/data/delta/dws_airport_hourly"),
    "country_5min": os.getenv("GOLD_COUNTRY_PATH", "/data/delta/gold_country_5min"),
    "overview_5min": os.getenv("GOLD_OVERVIEW_PATH", "/data/delta/gold_overview_5min"),
}

WINDOW_MINUTES = metrics.WINDOW_MINUTES
TS_FMT = "yyyy-MM-dd HH:mm:ss"

# 每张表的列，顺序要和 dws_metrics 里 select 的顺序一致
TABLE_COLUMNS = {
    "airport_5min": metrics.AIRPORT_COLUMNS,
    "airport_hourly": metrics.HOURLY_COLUMNS,
    "country_5min": metrics.COUNTRY_COLUMNS,
    "overview_5min": metrics.OVERVIEW_COLUMNS,
}

# 一次算一组表，它们的区间和重建条件是共同的
GROUPS = {
    "5min": ["airport_5min", "country_5min", "overview_5min"],
    "hourly": ["airport_hourly"],
}

# 增量默认往回看多久：把迟到数据和上次没算完的窗口一起覆盖掉。
# 5 分钟表看 1 小时（12 个窗口），小时表看 6 小时（给晚到的起降事件留余量）。
LOOKBACK_MINUTES = int(os.getenv("DWS_LOOKBACK_MINUTES", "60"))
LOOKBACK_HOURS = int(os.getenv("DWS_LOOKBACK_HOURS", "6"))


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DWS-Batch")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        # dt 和窗口都按 UTC，命令行给的时间也按 UTC 解释
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


# ================================================================
#  读上游
# ================================================================
def _require(spark: SparkSession, path: str, name: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        sys.exit(f"[dws] 找不到{name} {path}。先跑实时作业 rt_pipeline.py 写入数据。")


def _needs_rebuild(spark: SparkSession, group: str) -> bool:
    """这一组表里有没有表的列和代码对不上（M4 改名、加列之后就是这种情况）。"""
    for name in GROUPS[group]:
        path = TABLE_PATHS[name]
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        existing = set(spark.read.format("delta").load(path).columns)
        if existing != set(TABLE_COLUMNS[name]):
            return True
    return False


def read_states(spark: SparkSession, start: str, end: str) -> DataFrame:
    """区间内的空中记录。地面记录不进 DWS，见文件开头的说明。"""
    return (
        spark.read.format("delta").load(DWD_STATE_PATH)
        .where(f"event_time >= TIMESTAMP '{start}' AND event_time < TIMESTAMP '{end}'")
        .where("NOT on_ground")
    )


def read_events(spark: SparkSession, start: str, end: str) -> DataFrame:
    return (
        spark.read.format("delta").load(DWD_EVENT_PATH)
        .where(f"event_time >= TIMESTAMP '{start}' AND event_time < TIMESTAMP '{end}'")
    )


def data_clock(spark: SparkSession) -> str:
    """数据时钟：状态明细里最晚的快照时间。只算到它为止，机器时间不参与。"""
    row = (
        spark.read.format("delta").load(DWD_STATE_PATH)
        .agg(F.date_format(F.max("event_time"), TS_FMT).alias("t"))
        .first()
    )
    if not row or not row["t"]:
        sys.exit(f"[dws] {DWD_STATE_PATH} 是空的，没有可算的数据。")
    return row["t"]


def first_source_time(spark: SparkSession, path: str) -> str:
    """上游表里最早的一条记录时间：整表重建时从这里开始算。"""
    row = (
        spark.read.format("delta").load(path)
        .agg(F.date_format(F.min("event_time"), TS_FMT).alias("t"))
        .first()
    )
    return row["t"] if row and row["t"] else "1970-01-01 00:00:00"


def last_window(spark: SparkSession, path: str, col: str) -> str | None:
    """表里已经算到的最晚窗口起点；表不存在或为空返回 None。"""
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    row = (
        spark.read.format("delta").load(path)
        .agg(F.date_format(F.max(col), TS_FMT).alias("t"))
        .first()
    )
    return row["t"] if row and row["t"] else None


# ================================================================
#  区间解析
# ================================================================
def _shift(text: str, **kwargs) -> str:
    return (datetime.strptime(text, "%Y-%m-%d %H:%M:%S") + timedelta(**kwargs)) \
        .strftime("%Y-%m-%d %H:%M:%S")


def resolve_interval(spark: SparkSession, table: str, col: str, start: str | None,
                     end: str | None, clock: str, snap_minutes: int,
                     lookback_minutes: int, first_source: str) -> tuple[str, str]:
    """算出这次要重算的区间，两端都对齐到窗口边界。

    指定了 start/end 就照办（这是 backfill 和验收复算用的）；没指定就是增量：
      end   = 数据时钟向下取整（只算完整的窗口）
      start = min(表里最晚的窗口, end - lookback)，表不存在就从数据起点开始
    """
    snap = (lambda t: floor_hours(t)) if snap_minutes == 60 \
        else (lambda t: floor_minutes(t, snap_minutes))

    end_snapped = snap(end or clock)
    if start:
        start_snapped = snap(start)
    else:
        last = last_window(spark, TABLE_PATHS[table], col)
        start_snapped = snap(last) if last else snap(first_source)
        earliest = _shift(end_snapped, minutes=-lookback_minutes)
        if start_snapped > earliest:
            start_snapped = earliest
        floor_to_start = snap(first_source)
        if start_snapped < floor_to_start:
            start_snapped = floor_to_start
    return start_snapped, end_snapped


# ================================================================
#  写表：replaceWhere 覆盖区间，区间外不动
# ================================================================
def _replace_condition(col: str, start: str, end: str) -> str:
    # dt 是分区列，带上它 Delta 才能先按分区裁剪，不必扫整张表
    last_day = _shift(end, seconds=-1)[:10]
    return (f"dt BETWEEN DATE '{start[:10]}' AND DATE '{last_day}' "
            f"AND {col} >= TIMESTAMP '{start}' AND {col} < TIMESTAMP '{end}'")


def write_interval(spark: SparkSession, df: DataFrame, path: str, col: str,
                   start: str, end: str, name: str, rows: int) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        (df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").partitionBy("dt").save(path))
        print(f"[dws] {name}: 首次建表，{rows} 行")
        return

    existing = spark.read.format("delta").load(path)
    if set(existing.columns) != set(df.columns):
        # 代码改了列名或加了列。DWS 是从上游算出来的派生数据，整表重建最省事，
        # 也不会留下"新列全空"的历史行。
        print(f"[dws] {name}: 表结构变了（{len(existing.columns)} 列 → {len(df.columns)} 列），整表重建")
        (df.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").partitionBy("dt").save(path))
        print(f"[dws] {name}: 重建完成，{rows} 行")
        return

    cond = _replace_condition(col, start, end)
    (df.write.format("delta").mode("overwrite")
     .option("replaceWhere", cond).save(path))


# ================================================================
#  run / backfill：算一段区间
# ================================================================
def compute_airport_5min(spark: SparkSession, start: str, end: str) -> dict:
    states = read_states(spark, start, end)
    return {
        "airport_5min": metrics.airport_5min(states),
        "country_5min": metrics.country_5min(states),
        "overview_5min": metrics.overview_5min(states),
    }


def compute_hourly(spark: SparkSession, start: str, end: str) -> DataFrame:
    return metrics.airport_hourly(read_events(spark, start, end))


def run(spark: SparkSession, start: str | None = None, end: str | None = None,
        only: str | None = None) -> None:
    _require(spark, DWD_STATE_PATH, "状态明细")
    clock = data_clock(spark)
    print("=" * 66)
    print(f"  DWS 批处理：数据时钟 {clock} UTC")
    print("=" * 66)

    tasks = []
    if only in (None, "5min"):
        first = first_source_time(spark, DWD_STATE_PATH)
        if _needs_rebuild(spark, "5min"):
            start = first
            print("[dws] 5 分钟表结构有变，这次整表重建（区间自动扩到全部历史）")
        s5, e5 = resolve_interval(
            spark, "airport_5min", "window_start", start, end, clock,
            WINDOW_MINUTES, LOOKBACK_MINUTES, first)
        tasks.append((s5, e5, "5min"))
    if only in (None, "hourly"):
        _require(spark, DWD_EVENT_PATH, "起降事件表")
        first = first_source_time(spark, DWD_EVENT_PATH)
        if _needs_rebuild(spark, "hourly"):
            start = first
            print("[dws] 小时表结构有变，这次整表重建（区间自动扩到全部历史）")
        sh, eh = resolve_interval(
            spark, "airport_hourly", "hour_start", start, end, clock,
            60, LOOKBACK_HOURS * 60, first)
        tasks.append((sh, eh, "hourly"))

    for s, e, kind in tasks:
        if s >= e:
            print(f"[dws] {kind}: 区间 {s} → {e} 为空（数据还没到新的窗口边界），跳过")
            continue

        started = time.time()
        if kind == "5min":
            outputs = compute_airport_5min(spark, s, e)
        else:
            outputs = {"airport_hourly": compute_hourly(spark, s, e)}

        for name, df in outputs.items():
            rows = df.count()
            write_interval(spark, df, TABLE_PATHS[name],
                           "hour_start" if name == "airport_hourly" else "window_start",
                           s, e, name, rows)
            print(f"[dws] {name}: [{s}, {e}) UTC → {rows} 行")

        print(f"[dws] {kind}: 区间 [{s}, {e}) 用时 {time.time() - started:.1f} 秒")


# ================================================================
#  verify：表里的结果 vs 现在重算的结果
# ================================================================
def _diff(spark: SparkSession, expected: DataFrame, path: str, start: str, end: str,
          name: str) -> tuple[int, int, int, int]:
    col = "hour_start" if name == "airport_hourly" else "window_start"
    actual = (spark.read.format("delta").load(path)
              .where(f"{col} >= TIMESTAMP '{start}' AND {col} < TIMESTAMP '{end}'")
              .select(*expected.columns))
    expected = expected.select(*actual.columns)
    n_expected, n_actual = expected.count(), actual.count()
    only_table = actual.exceptAll(expected).count()
    only_rebuild = expected.exceptAll(actual).count()
    return n_expected, n_actual, only_table, only_rebuild


def verify(spark: SparkSession, start: str, end: str) -> int:
    for group in GROUPS:
        for name in GROUPS[group]:
            _require(spark, TABLE_PATHS[name], "DWS 表")
    clock = data_clock(spark)
    s5, e5 = resolve_interval(spark, "airport_5min", "window_start", start, end, clock,
                              WINDOW_MINUTES, LOOKBACK_MINUTES, start)
    sh, eh = resolve_interval(spark, "airport_hourly", "hour_start", start, end, clock,
                              60, LOOKBACK_HOURS * 60, start)

    print("=" * 66)
    print(f"  DWS 对账  [{start}, {end}) UTC")
    print("=" * 66)

    checks = list(compute_airport_5min(spark, s5, e5).items()) + [
        ("airport_hourly", compute_hourly(spark, sh, eh)),
    ]
    failed = False
    total_expected = 0
    for name, expected in checks:
        n_exp, n_act, only_t, only_r = _diff(spark, expected, TABLE_PATHS[name],
                                             s5 if name != "airport_hourly" else sh,
                                             e5 if name != "airport_hourly" else eh, name)
        total_expected += n_exp
        ok = only_t == 0 and only_r == 0
        failed = failed or not ok
        print(f"[dws-verify] {name:<16} 重算 {n_exp:>6} 行，表内 {n_act:>6} 行，"
              f"只在表里 {only_t}，只在重算里 {only_r} {'' if ok else '← 不一致'}")

    if total_expected == 0:
        print("[dws-verify] 不能算通过：区间内没有可比的数据，换一个两边都有数据的区间")
        return 1
    if failed:
        print("[dws-verify] 不一致：先确认实时作业已经写入这段状态明细，再重跑 backfill")
        return 1
    print("[dws-verify] 通过：同一区间重算的结果和表里完全一致")
    return 0


# ================================================================
#  命令行
# ================================================================
def parse_utc(text: str) -> str:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"时间格式不对：{text}，例如 2026-10-20 00:00")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="DWS 批处理：增量 / 区间回刷 / 对账")
    p.add_argument("mode", nargs="?", default="run", choices=["run", "backfill", "verify"])
    p.add_argument("--start", type=parse_utc, help="区间开始，UTC，包含")
    p.add_argument("--end", type=parse_utc, help="区间结束，UTC，不包含")
    p.add_argument("--only", choices=["5min", "hourly"], help="只算其中一类表")
    args = p.parse_args(argv)
    if args.mode != "run" and not (args.start and args.end):
        p.error("backfill 和 verify 需要 --start 和 --end")
    if args.start and args.end and args.start >= args.end:
        p.error("--start 必须早于 --end")
    return args


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.mode == "verify":
        code = verify(spark, args.start, args.end)
        spark.stop()
        sys.exit(code)

    run(spark, args.start, args.end, args.only)
    spark.stop()


if __name__ == "__main__":
    main()
