"""
DWD 状态明细 dwd_flight_state：流式写入、按区间回刷、流批对账
================================================================
一行 = 一架飞机在一个快照时刻的清洗后状态（含地面记录）。
主键 (icao24, event_time)，按 UTC 日期 dt 分区。
三种模式用的是同一份清洗规则 flight_rules.clean_states：

  stream    读 bronze 的 Delta 流，每 5 秒一个微批：清洗 → 按主键 MERGE，只插入表里没有的行
  backfill  按时间区间从 bronze 重算，用 replaceWhere 只覆盖这个区间，其他时段不动
  verify    同一区间：表里的现有结果 vs 从 bronze 现算的结果，两个方向 EXCEPT ALL，差异必须是 0

跑法（在 spark 容器里；时间按 UTC 解释，区间左闭右开）：
  spark-submit jobs/dwd_state.py                          # stream，常驻
  spark-submit jobs/dwd_state.py stream --available-now   # 处理完积压就退出
  spark-submit jobs/dwd_state.py backfill --start "2026-10-17 08:00" --end "2026-10-17 09:00"
  spark-submit jobs/dwd_state.py verify   --start "2026-10-17 08:00" --end "2026-10-17 09:00"

★ 为什么用 foreachBatch + MERGE，而不是直接 append
  流作业失败重启后，最后一个微批会按 checkpoint 重放。直接 append 会把这批写两遍；
  MERGE 按主键只插入表里没有的行，重放时全部命中、一行不插，写入是幂等的。
  同一个机制也挡住采集端的重复：Kafka 重试、同一张快照拉了两次，主键相同就只留一条。

★ 为什么 MERGE 条件里带着时间范围
  只写 icao24、event_time 相等，Delta 要把当天分区的文件全扫一遍去找匹配，
  表越大每批越慢。把这一批的最早、最晚时间作为常量写进条件，Delta 用每个文件的
  min/max 统计直接跳过不相关的文件，每批耗时就不随表的增长而变长。
  这个范围不会漏掉匹配：主键相等意味着 event_time 相等，一定落在这一批的时间范围内。

★ 回刷对下游的影响
  replaceWhere 会产生"删旧文件 + 写新文件"的提交。以后流式读本表的作业（航段、最新状态）
  要加 skipChangeCommits，回刷之后对下游也做同区间回刷。
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

from flight_rules import KEY_COLUMNS, airports_column, clean_states, load_airports

# ---------------- 路径与参数（环境变量可覆盖，本地测试时用）----------------
BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/delta/bronze_flights")
DWD_STATE_PATH = os.getenv("DWD_STATE_PATH", "/data/delta/dwd_flight_state")
CHECKPOINT_PATH = os.getenv("DWD_STATE_CHECKPOINT", "/data/checkpoint/dwd_flight_state")
TRIGGER_INTERVAL = os.getenv("DWD_TRIGGER_INTERVAL", "5 seconds")

# MERGE 遇到并发冲突时的最多尝试次数。MERGE 是幂等的，重试是安全的。
MERGE_ATTEMPTS = 3
# Delta 乐观并发控制下，两个事务改到同一批文件时，后提交的一方抛出这些异常之一
DELTA_CONFLICTS = (
    ConcurrentAppendException,
    ConcurrentDeleteReadException,
    ConcurrentDeleteDeleteException,
    ConcurrentTransactionException,
    ConcurrentWriteException,
    MetadataChangedException,
    ProtocolChangedException,
)

TS_FMT = "yyyy-MM-dd HH:mm:ss"

MERGE_SQL = """
MERGE INTO delta.`{path}` AS t
USING dwd_state_batch AS s
ON  t.dt BETWEEN DATE '{d_min}' AND DATE '{d_max}'
AND t.event_time BETWEEN TIMESTAMP '{t_min}' AND TIMESTAMP '{t_max}'
AND t.icao24 = s.icao24
AND t.event_time = s.event_time
WHEN NOT MATCHED THEN INSERT *
"""


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DWD-Flight-State")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        # dt 按 UTC 日期分区，命令行给的时间也按 UTC 解释
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def with_load_time(df: DataFrame) -> DataFrame:
    """加上写入 DWD 的时间。它随写入时刻变化，对账时不比这一列。"""
    return df.withColumn("dwd_load_time", F.current_timestamp())


def read_bronze(spark: SparkSession) -> DataFrame:
    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        sys.exit(f"[dwd] 找不到 bronze 表 {BRONZE_PATH}，先运行 ods_bronze.py 写入数据。")
    return spark.read.format("delta").load(BRONZE_PATH)


def ensure_table(spark: SparkSession, airports) -> None:
    """表不存在就建。表结构直接取 clean_states 的输出，代码和表结构不会对不上。"""
    empty = with_load_time(clean_states(read_bronze(spark).limit(0), airports))
    # 主键列声明 NOT NULL：万一有空值漏过清洗，Delta 会在写入时直接报错
    schema = StructType([
        StructField(f.name, f.dataType, nullable=f.name not in KEY_COLUMNS)
        for f in empty.schema.fields
    ])
    (
        DeltaTable.createIfNotExists(spark)
        .location(DWD_STATE_PATH)
        .addColumns(schema)
        .partitionedBy("dt")
        .execute()
    )


# ================================================================
#  stream：foreachBatch + MERGE
# ================================================================
def make_batch_writer(airports):
    def upsert_batch(batch_df: DataFrame, batch_id: int) -> None:
        started = time.time()
        spark = batch_df.sparkSession
        # 这一批要用好几次（计数、算时间范围、MERGE），先缓存，避免重复读文件和重复清洗
        batch_df.persist()
        cleaned = with_load_time(clean_states(batch_df, airports)).persist()
        try:
            n_in = batch_df.count()
            # 时间范围在 Spark 里格式化成字符串再取回，不直接 collect 成 Python datetime：
            # PySpark 会按 driver 进程的本地时区转换时间戳，机器时区不是 UTC 时会差几个小时
            stats = cleaned.agg(
                F.count(F.lit(1)).alias("n"),
                F.date_format(F.min("event_time"), TS_FMT).alias("t_min"),
                F.date_format(F.max("event_time"), TS_FMT).alias("t_max"),
            ).first()
            if stats["n"] == 0:
                print(f"[dwd-stream] 批 {batch_id}：读入 {n_in} 条，清洗后为空，跳过")
                return

            cleaned.createOrReplaceTempView("dwd_state_batch")
            merge_sql = MERGE_SQL.format(
                path=DWD_STATE_PATH,
                d_min=stats["t_min"][:10], d_max=stats["t_max"][:10],
                t_min=stats["t_min"], t_max=stats["t_max"],
            )
            inserted = merge_with_retry(spark, merge_sql, batch_id)

            dropped = n_in - stats["n"]
            # 同一天只显示时分秒；追历史时一批可能跨好几天，就显示完整时间
            same_day = stats["t_min"][:10] == stats["t_max"][:10]
            span = (f"{stats['t_min'][11:]}–{stats['t_max'][11:]}" if same_day
                    else f"{stats['t_min']} – {stats['t_max']}")
            # 清洗后有、但没插进去的行，是表里已经有的主键：重放或采集端重复
            existed = f"（{stats['n'] - inserted} 条表里已有）" if isinstance(inserted, int) else ""
            print(
                f"[dwd-stream] 批 {batch_id}：读入 {n_in} 条，"
                f"过滤和去重掉 {dropped} 条（{dropped / n_in:.1%}），"
                f"新插入 {inserted} 条{existed}，"
                f"快照 {span} UTC，"
                f"耗时 {time.time() - started:.1f} 秒"
            )
        finally:
            cleaned.unpersist()
            batch_df.unpersist()

    return upsert_batch


def merge_with_retry(spark: SparkSession, merge_sql: str, batch_id: int):
    """执行 MERGE，遇到 Delta 并发冲突（例如同时在回刷同一天）就重试。返回插入行数。"""
    for attempt in range(1, MERGE_ATTEMPTS + 1):
        try:
            rows = spark.sql(merge_sql).collect()
            # Delta 的 MERGE 会返回一行影响行数；拿不到就显示问号，不影响写入
            return rows[0].asDict().get("num_inserted_rows", "?") if rows else "?"
        except DELTA_CONFLICTS as e:
            if attempt == MERGE_ATTEMPTS:
                raise
            print(f"[dwd-stream] 批 {batch_id}：MERGE 遇到并发冲突（{type(e).__name__}），"
                  f"{attempt} 秒后第 {attempt} 次重试")
            time.sleep(attempt)


def run_stream(spark: SparkSession, airports, available_now: bool) -> None:
    ensure_table(spark, airports)

    writer = (
        spark.readStream.format("delta").load(BRONZE_PATH)
        .writeStream
        .queryName("dwd_flight_state")
        .foreachBatch(make_batch_writer(airports))
        .option("checkpointLocation", CHECKPOINT_PATH)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
        trigger_desc = "availableNow（处理完当前积压就退出）"
    else:
        writer = writer.trigger(processingTime=TRIGGER_INTERVAL)
        trigger_desc = f"每 {TRIGGER_INTERVAL}"

    print("=" * 64)
    print("  DWD 状态明细（流式）：bronze → 清洗 → MERGE → dwd_flight_state")
    print(f"  读取     : {BRONZE_PATH}")
    print(f"  写入     : {DWD_STATE_PATH}")
    print(f"  checkpoint: {CHECKPOINT_PATH}")
    print(f"  触发     : {trigger_desc}")
    print("=" * 64)
    query = writer.start()
    query.awaitTermination()


# ================================================================
#  backfill / verify：按时间区间
# ================================================================
def bronze_range(start: str, end: str) -> str:
    return f"event_time >= TIMESTAMP '{start}' AND event_time < TIMESTAMP '{end}'"


def table_range(start: str, end: str) -> str:
    # dt 条件让 Delta 只看相关的日期分区；event_time 条件精确到区间
    return (f"dt BETWEEN DATE '{start[:10]}' AND DATE '{end[:10]}' AND "
            + bronze_range(start, end))


def run_backfill(spark: SparkSession, airports, start: str, end: str) -> None:
    ensure_table(spark, airports)
    started = time.time()
    before = spark.read.format("delta").load(DWD_STATE_PATH).where(table_range(start, end)).count()

    rebuilt = with_load_time(
        clean_states(read_bronze(spark).where(bronze_range(start, end)), airports)
    )
    # replaceWhere：在一个事务里删掉区间内的旧数据、写入新算的数据。
    # 区间外的数据不动；读者要么看到回刷前、要么看到回刷后，不会看到一半。
    (
        rebuilt.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", table_range(start, end))
        .save(DWD_STATE_PATH)
    )

    after = spark.read.format("delta").load(DWD_STATE_PATH).where(table_range(start, end)).count()
    print(f"[dwd-backfill] [{start}, {end}) UTC：原有 {before} 条 → 回刷后 {after} 条，"
          f"耗时 {time.time() - started:.1f} 秒")


def run_verify(spark: SparkSession, airports, start: str, end: str) -> int:
    """对账：表里的结果和从 bronze 现算的结果逐行比较。一致返回 0，否则返回 1。"""
    if not DeltaTable.isDeltaTable(spark, DWD_STATE_PATH):
        sys.exit(f"[dwd-verify] 找不到 {DWD_STATE_PATH}，先跑 stream 或 backfill。")

    expected = clean_states(read_bronze(spark).where(bronze_range(start, end)), airports).persist()
    table = spark.read.format("delta").load(DWD_STATE_PATH)
    actual = table.where(table_range(start, end)).select(*expected.columns).persist()

    n_expected, n_actual = expected.count(), actual.count()
    only_in_table = actual.exceptAll(expected)
    only_in_rebuild = expected.exceptAll(actual)
    n_only_table, n_only_rebuild = only_in_table.count(), only_in_rebuild.count()
    dup_keys = actual.groupBy(*KEY_COLUMNS).count().where("count > 1").count()
    latest = table.agg(F.date_format(F.max("event_time"), TS_FMT)).first()[0]

    print("=" * 64)
    print(f"  流批对账  [{start}, {end}) UTC")
    print(f"  表内行数（流式写入）  : {n_actual}")
    print(f"  现算行数（批量重算）  : {n_expected}")
    print(f"  只在表里的行          : {n_only_table}")
    print(f"  只在现算结果里的行    : {n_only_rebuild}")
    print(f"  表内主键重复          : {dup_keys}")
    print("=" * 64)

    if n_only_table == 0 and n_only_rebuild == 0 and dup_keys == 0:
        print("[dwd-verify] 通过：同一区间的流式结果与批量重算结果完全一致")
        return 0

    if latest is not None and latest < end:
        print(f"[dwd-verify] 注意：表内最新快照是 {latest}，早于区间结束时间。"
              "流作业可能还没处理完这段数据，换一个更早的区间再比。")
    print("[dwd-verify] 不一致，样例：")
    only_in_table.select(*KEY_COLUMNS, "callsign", "kafka_ts").show(5, truncate=False)
    only_in_rebuild.select(*KEY_COLUMNS, "callsign", "kafka_ts").show(5, truncate=False)
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
    p = argparse.ArgumentParser(description="dwd_flight_state：流式写入 / 区间回刷 / 流批对账")
    p.add_argument("mode", nargs="?", default="stream", choices=["stream", "backfill", "verify"])
    p.add_argument("--available-now", action="store_true",
                   help="stream 模式：处理完当前积压就退出")
    p.add_argument("--start", type=parse_utc, help="区间开始，UTC，包含")
    p.add_argument("--end", type=parse_utc, help="区间结束，UTC，不包含")
    args = p.parse_args(argv)
    if args.mode != "stream":
        if not (args.start and args.end):
            p.error("backfill 和 verify 需要 --start 和 --end")
        if args.start >= args.end:
            p.error("--start 必须早于 --end")
    return args


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    airports = airports_column(load_airports())

    if args.mode == "stream":
        run_stream(spark, airports, args.available_now)
    elif args.mode == "backfill":
        run_backfill(spark, airports, args.start, args.end)
    else:
        code = run_verify(spark, airports, args.start, args.end)
        spark.stop()
        sys.exit(code)


if __name__ == "__main__":
    main()
