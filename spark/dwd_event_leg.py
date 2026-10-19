"""
DWD 起降事件 dwd_airport_event 与航段 dwd_flight_leg：流式写入、全量重建、对账
==============================================================================
规则都在 flight_legs.py，这个作业只负责读写 Delta。

  stream   流式读状态明细的新增行，每 5 秒一批：判起降，事件按主键只插入；更新航段，按 leg_id 整行 upsert
  rebuild  从状态明细全量重算两张表并覆盖。改了规则、或状态明细做过回刷之后用，先停掉流作业
  verify   全量重算一遍，和表里的结果逐行比：事件按事件时间、航段按开始时间取区间

跑法（在 spark 容器里；时间按 UTC 解释，区间左闭右开）：
  spark-submit jobs/dwd_event_leg.py                          # stream，常驻
  spark-submit jobs/dwd_event_leg.py stream --available-now   # 处理完积压就退出；第一次跑用它把历史建出来
  spark-submit jobs/dwd_event_leg.py rebuild
  spark-submit jobs/dwd_event_leg.py verify --start "2026-10-17 08:00" --end "2026-10-17 09:00"

★ 为什么流式读状态明细要加 skipChangeCommits
  状态明细回刷（dwd_state.py backfill）用 replaceWhere，提交里有"删旧文件"。流式读 Delta 遇到这种
  提交默认会报错；加上这个选项就跳过它。代价是回刷的改动不会自动传到这里，回刷之后要 rebuild。

★ 为什么 maxFilesPerTrigger 设得很大
  航段判定依赖每架飞机记录的先后顺序。第一次启动时历史数据在一个微批里处理完，
  不会被拆成几批、打乱顺序。之后每批只是新增的几个文件，不受影响。

★ 代码里的表结构变了怎么办
  三种模式启动时都先拿表结构和代码比，不一致就退出，提示删掉两张表再 rebuild。没有自动改表：
  Delta 的 CREATE OR REPLACE 对按路径建的表不会换表结构，实测会报 DELTA_CREATE_TABLE_SCHEME_MISMATCH。
  这两张表都能从状态明细全量重算，删表重建 30 秒左右，比维护一套改表逻辑可靠。

★ 一批写两张表，中途失败重放会不会写重
  两次 MERGE 不在一个事务里，所以每一次都要能安全地重做：起降事件只插入表里没有的主键；
  航段的合并规则都是单调的（取较大值、结束状态优先），同一批重放得到的是同一组行。
  单测里模拟了"事件写入后航段 MERGE 失败再重放"，结果不变。
"""

import argparse
import os
import sys
import time
from datetime import datetime

from delta.exceptions import (
    ConcurrentAppendException,
    ConcurrentDeleteDeleteException,
    ConcurrentDeleteReadException,
    ConcurrentTransactionException,
    ConcurrentWriteException,
    MetadataChangedException,
    ProtocolChangedException,
)
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType

from flight_legs import (
    EVENT_KEY, LEG_COLUMNS, LEG_KEY, STATE_COLUMNS, TS_FMT, compute_all, stream_step,
)

# ---------------- 路径与参数（环境变量可覆盖）----------------
DWD_STATE_PATH = os.getenv("DWD_STATE_PATH", "/data/delta/dwd_flight_state")
EVENT_PATH = os.getenv("DWD_EVENT_PATH", "/data/delta/dwd_airport_event")
LEG_PATH = os.getenv("DWD_LEG_PATH", "/data/delta/dwd_flight_leg")
CHECKPOINT_PATH = os.getenv("DWD_EVENT_LEG_CHECKPOINT", "/data/checkpoint/dwd_event_leg")
TRIGGER_INTERVAL = os.getenv("DWD_EVENT_LEG_TRIGGER", "5 seconds")
MAX_FILES_PER_TRIGGER = os.getenv("DWD_EVENT_LEG_MAX_FILES", "100000")

MERGE_ATTEMPTS = 3
DELTA_CONFLICTS = (
    ConcurrentAppendException,
    ConcurrentDeleteReadException,
    ConcurrentDeleteDeleteException,
    ConcurrentTransactionException,
    ConcurrentWriteException,
    MetadataChangedException,
    ProtocolChangedException,
)

# 时间范围写成常量，Delta 用每个文件的 min/max 统计跳过无关文件（同 dwd_state.py）
EVENT_MERGE_SQL = """
MERGE INTO delta.`{path}` AS t
USING m2_events AS s
ON  t.dt BETWEEN DATE '{d_min}' AND DATE '{d_max}'
AND t.event_time BETWEEN TIMESTAMP '{t_min}' AND TIMESTAMP '{t_max}'
AND t.icao24 = s.icao24
AND t.event_time = s.event_time
AND t.event_type = s.event_type
WHEN NOT MATCHED THEN INSERT *
"""

# 合并逻辑已经在 flight_legs.apply_leg_updates 里算好，这里只按 leg_id 整行写入
LEG_MERGE_SQL = """
MERGE INTO delta.`{path}` AS t
USING m2_legs AS s
ON  t.dt >= DATE '{d_min}'
AND t.leg_id = s.leg_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DWD-Event-Leg")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        # dt 按 UTC 日期分区，命令行给的时间也按 UTC 解释
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def with_load_time(df: DataFrame) -> DataFrame:
    """写入时间。它随写入时刻变化，对账时不比这一列。"""
    return df.withColumn("load_time", F.current_timestamp())


def read_states(spark: SparkSession) -> DataFrame:
    if not DeltaTable.isDeltaTable(spark, DWD_STATE_PATH):
        sys.exit(f"[m2] 找不到状态明细 {DWD_STATE_PATH}，先运行 dwd_state.py。")
    return spark.read.format("delta").load(DWD_STATE_PATH)


def expected_schemas(spark: SparkSession):
    """两张表按代码应有的表结构：直接取 flight_legs 的输出加 load_time，主键列 NOT NULL。"""
    events, legs = compute_all(read_states(spark).limit(0), "1970-01-01 00:00:00")
    result = []
    for path, df, keys in ((EVENT_PATH, events, EVENT_KEY), (LEG_PATH, legs, LEG_KEY)):
        df = with_load_time(df)
        result.append((path, StructType([
            StructField(f.name, f.dataType, nullable=f.name not in keys) for f in df.schema.fields
        ])))
    return result


def schema_diff(table: StructType, code: StructType) -> str:
    """表里的列和代码不一致时返回差在哪，一致返回空字符串。只比列名、类型和顺序，不比 NOT NULL。"""
    actual = [(f.name, f.dataType.simpleString()) for f in table.fields]
    expected = [(f.name, f.dataType.simpleString()) for f in code.fields]
    if actual == expected:
        return ""
    a, e = dict(actual), dict(expected)
    parts = []
    added = [n for n in e if n not in a]
    removed = [n for n in a if n not in e]
    changed = [n for n in e if n in a and a[n] != e[n]]
    if added:
        parts.append("代码里新增 " + "、".join(added))
    if removed:
        parts.append("代码里去掉了 " + "、".join(removed))
    if changed:
        parts.append("类型变了 " + "、".join(changed))
    return "；".join(parts) or "列的顺序变了"


def check_tables(spark: SparkSession, tag: str) -> None:
    """表已存在、但表结构和代码不一致时退出，给出删表重建的命令（见文件开头的说明）。

    流作业的 MERGE 用 UPDATE SET * / INSERT *，表里少了列会直接失败；verify 会选不到新列。
    与其跑到一半报 Delta 的错，不如启动时先查一遍。
    """
    for path, schema in expected_schemas(spark):
        if not DeltaTable.isDeltaTable(spark, path):
            continue
        diff = schema_diff(spark.read.format("delta").load(path).schema, schema)
        if diff:
            sys.exit(f"[{tag}] {path} 的表结构和代码不一致（{diff}）。先停掉本作业，删掉两张表再 rebuild：\n"
                     f"  docker exec spark rm -rf {EVENT_PATH} {LEG_PATH}\n"
                     "  checkpoint 不用删。")


def ensure_tables(spark: SparkSession, tag: str) -> None:
    """表结构和代码一致才继续；表不存在就按代码建，主键列 NOT NULL，按 dt 分区。"""
    check_tables(spark, tag)
    for path, schema in expected_schemas(spark):
        (
            DeltaTable.createIfNotExists(spark)
            .location(path)
            .addColumns(schema)
            .partitionedBy("dt")
            .execute()
        )


def merge_with_retry(spark: SparkSession, sql: str, batch_id: int, name: str) -> dict:
    """执行 MERGE，遇到 Delta 并发冲突就重试。MERGE 是幂等的，重试安全。返回影响行数。"""
    for attempt in range(1, MERGE_ATTEMPTS + 1):
        try:
            rows = spark.sql(sql).collect()
            return rows[0].asDict() if rows else {}
        except DELTA_CONFLICTS as e:
            if attempt == MERGE_ATTEMPTS:
                raise
            print(f"[m2-stream] 批 {batch_id}：{name} MERGE 遇到并发冲突（{type(e).__name__}），"
                  f"{attempt} 秒后第 {attempt} 次重试")
            time.sleep(attempt)


# ================================================================
#  stream
# ================================================================
class Materializer:
    """flight_legs 的 cache 参数：persist 并立刻 count，批处理结束时统一释放。

    立刻 count 是为了让后面的查询直接读缓存，优化器拿到的也是实际大小，
    不会把各分支的估算值连乘到天文数字。
    """

    def __init__(self):
        self.cached = []

    def __call__(self, df: DataFrame) -> DataFrame:
        df = df.persist()
        df.count()
        self.cached.append(df)
        return df

    def release(self) -> None:
        for df in reversed(self.cached):
            df.unpersist()
        self.cached.clear()


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    started = time.time()
    spark = batch_df.sparkSession
    cache = Materializer()
    try:
        batch = cache(batch_df.select(*STATE_COLUMNS))
        step = stream_step(batch, read_states(spark), spark.read.format("delta").load(LEG_PATH), cache)
        if step is None:
            return
        events, leg_rows, b = step
        # 先物化：航段行是从航段表算出来的，写回同一张表之前固定下来
        events = cache(with_load_time(events))
        leg_rows = cache(with_load_time(leg_rows))
        by_type = count_events(events)
        by_leg = count_legs(leg_rows)

        if by_type:
            events.createOrReplaceTempView("m2_events")
            merge_with_retry(spark, EVENT_MERGE_SQL.format(
                path=EVENT_PATH, d_min=b["t_min"][:10], d_max=b["t_max"][:10],
                t_min=b["t_min"], t_max=b["t_max"]), batch_id, "起降事件")
        if by_leg:
            d_min = leg_rows.agg(F.date_format(F.min("dt"), "yyyy-MM-dd")).first()[0]
            leg_rows.createOrReplaceTempView("m2_legs")
            merge_with_retry(spark, LEG_MERGE_SQL.format(path=LEG_PATH, d_min=d_min),
                             batch_id, "航段")

        same_day = b["t_min"][:10] == b["t_max"][:10]
        span = (f"{b['t_min'][11:]}–{b['t_max'][11:]}" if same_day
                else f"{b['t_min']} – {b['t_max']}")
        print(
            f"[m2-stream] 批 {batch_id}：读入 {b['n']} 条，"
            f"落地 {by_type.get('LANDING', 0)}、起飞 {by_type.get('TAKEOFF', 0)}；"
            f"写入航段 {describe_legs(by_leg)}，快照 {span} UTC，耗时 {time.time() - started:.1f} 秒"
        )
    finally:
        cache.release()


def count_events(df: DataFrame) -> dict:
    """按事件类型计数：{'LANDING': n, 'TAKEOFF': n}。"""
    return {r["event_type"]: r["n"]
            for r in df.groupBy("event_type").agg(F.count(F.lit(1)).alias("n")).collect()}


def count_legs(df: DataFrame) -> dict:
    """按 (状态, 丢失原因) 计数，一次聚合同时得到两种分布。"""
    return {(r["status"], r["lost_reason"]): r["n"]
            for r in df.groupBy("status", "lost_reason").agg(F.count(F.lit(1)).alias("n")).collect()}


def describe_legs(counts: dict) -> str:
    """'6 个（进行中 3、落地 1、丢失 2：疑似落地 1、飞出范围 1、信号丢失 0）' 这样的一句话。"""
    def n(status=None, reason=None):
        return sum(v for (s, r), v in counts.items()
                   if status in (None, s) and reason in (None, r))
    return (f"{n()} 个（进行中 {n('IN_AIR')}、落地 {n('LANDED')}、丢失 {n('LOST')}："
            f"疑似落地 {n('LOST', 'SUSPECTED_LANDING')}、飞出范围 {n('LOST', 'EXITED')}、"
            f"信号丢失 {n('LOST', 'SIGNAL_LOST')}）")


def run_stream(spark: SparkSession, available_now: bool) -> None:
    ensure_tables(spark, "m2-stream")
    writer = (
        spark.readStream.format("delta")
        .option("skipChangeCommits", "true")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .load(DWD_STATE_PATH)
        .writeStream
        .queryName("dwd_event_leg")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
        trigger_desc = "availableNow（处理完当前积压就退出）"
    else:
        writer = writer.trigger(processingTime=TRIGGER_INTERVAL)
        trigger_desc = f"每 {TRIGGER_INTERVAL}"

    print("=" * 64)
    print("  DWD 起降事件与航段（流式）：dwd_flight_state → 判起降 → 切航段")
    print(f"  读取      : {DWD_STATE_PATH}")
    print(f"  写入      : {EVENT_PATH}")
    print(f"              {LEG_PATH}")
    print(f"  checkpoint: {CHECKPOINT_PATH}")
    print(f"  触发      : {trigger_desc}")
    print("=" * 64)
    writer.start().awaitTermination()


# ================================================================
#  rebuild / verify
# ================================================================
def run_rebuild(spark: SparkSession) -> None:
    ensure_tables(spark, "m2-rebuild")
    started = time.time()
    cache = Materializer()
    try:
        events, legs = compute_all(read_states(spark), cache=cache)
        # 各覆盖一次：读者要么看到重建前、要么看到重建后，不会看到一半
        with_load_time(events).write.format("delta").mode("overwrite").save(EVENT_PATH)
        with_load_time(legs).write.format("delta").mode("overwrite").save(LEG_PATH)
    finally:
        cache.release()
    by_type = count_events(spark.read.format("delta").load(EVENT_PATH))
    by_leg = count_legs(spark.read.format("delta").load(LEG_PATH))
    print(f"[m2-rebuild] 起降事件 {sum(by_type.values())} 条（落地 {by_type.get('LANDING', 0)}、"
          f"起飞 {by_type.get('TAKEOFF', 0)}），航段 {describe_legs(by_leg)}，"
          f"耗时 {time.time() - started:.1f} 秒")


def compare(name: str, expected: DataFrame, actual: DataFrame, keys) -> bool:
    expected, actual = expected.persist(), actual.persist()
    n_exp, n_act = expected.count(), actual.count()
    only_table, only_rebuild = actual.exceptAll(expected), expected.exceptAll(actual)
    n_table, n_rebuild = only_table.count(), only_rebuild.count()
    dups = actual.groupBy(*keys).count().where("count > 1").count()
    print(f"  {name}：表内 {n_act}，重算 {n_exp}，只在表里 {n_table}，只在重算里 {n_rebuild}，主键重复 {dups}")
    if n_table or n_rebuild:
        only_table.show(5, truncate=False)
        only_rebuild.show(5, truncate=False)
    return n_exp + n_act > 0 and n_table == 0 and n_rebuild == 0 and dups == 0


def run_verify(spark: SparkSession, start: str, end: str) -> int:
    for path in (EVENT_PATH, LEG_PATH):
        if not DeltaTable.isDeltaTable(spark, path):
            sys.exit(f"[m2-verify] 找不到 {path}，先跑 stream 或 rebuild。")
    check_tables(spark, "m2-verify")

    states = read_states(spark)
    clock = states.agg(F.date_format(F.max("event_time"), TS_FMT)).first()[0]
    events, legs = compute_all(states, clock, cache=Materializer())
    in_range = (f"dt BETWEEN DATE '{start[:10]}' AND DATE '{end[:10]}' AND "
                f"{{col}} >= TIMESTAMP '{start}' AND {{col}} < TIMESTAMP '{end}'")
    event_table = spark.read.format("delta").load(EVENT_PATH)
    leg_table = spark.read.format("delta").load(LEG_PATH)
    leg_progress = leg_table.agg(F.date_format(F.max("last_seen"), TS_FMT)).first()[0]

    print("=" * 64)
    print(f"  起降与航段对账  [{start}, {end}) UTC")
    print(f"  状态明细最新快照 {clock}；航段表最新空中记录 {leg_progress}")
    ok_events = compare(
        "起降事件（按事件时间）",
        events.where(in_range.format(col="event_time")),
        event_table.where(in_range.format(col="event_time")).select(*events.columns),
        EVENT_KEY,
    )
    ok_legs = compare(
        "航段（按开始时间）",
        legs.where(in_range.format(col="start_time")),
        leg_table.where(in_range.format(col="start_time")).select(*LEG_COLUMNS),
        LEG_KEY,
    )
    print("=" * 64)
    if ok_events and ok_legs:
        print("[m2-verify] 通过：流式写入的起降事件和航段与全量重算完全一致")
        return 0
    print("[m2-verify] 不一致或区间内没有数据。流作业要先追平：停掉采集器，等 dwd_state 和本作业"
          "都不再出新批次，再对账；区间也要选有数据的时段。")
    return 1


# ================================================================
#  命令行
# ================================================================
def parse_utc(text: str) -> str:
    """把 '2026-10-17 08:00' 这类输入规范成 'yyyy-MM-dd HH:mm:ss'（按 UTC 理解）。"""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"时间格式不对：{text}，例如 2026-10-17 08:00")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="dwd_airport_event / dwd_flight_leg：流式写入 / 全量重建 / 对账")
    p.add_argument("mode", nargs="?", default="stream", choices=["stream", "rebuild", "verify"])
    p.add_argument("--available-now", action="store_true", help="stream 模式：处理完当前积压就退出")
    p.add_argument("--start", type=parse_utc, help="verify 区间开始，UTC，包含")
    p.add_argument("--end", type=parse_utc, help="verify 区间结束，UTC，不包含")
    args = p.parse_args(argv)
    if args.mode == "verify":
        if not (args.start and args.end):
            p.error("verify 需要 --start 和 --end")
        if args.start >= args.end:
            p.error("--start 必须早于 --end")
    return args


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    if args.mode == "stream":
        run_stream(spark, args.available_now)
    elif args.mode == "rebuild":
        run_rebuild(spark)
    else:
        code = run_verify(spark, args.start, args.end)
        spark.stop()
        sys.exit(code)


if __name__ == "__main__":
    main()
