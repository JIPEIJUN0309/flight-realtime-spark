"""
阶段四(2/2)：DWS（Gold）层 —— 流处理版 ★ 核心
================================================
Kafka → Structured Streaming（带 watermark）→ 5分钟窗口聚合 → Delta gold 表

这个作业是整个项目"实时流处理能力"的最强证据，主讲它。

★★★ 核心考点：watermark（水位线）★★★
--------------------------------------------------
【要解决的真实问题】
真实的流数据不会按时间顺序乖乖到达。一条 20:05 发生的航班数据，
可能因为网络延迟、Kafka 重试、采集抖动，20:07 才到达 Spark。

那么当 Spark 在算 "20:00-20:05 这个窗口的航班数" 时：
  - 如果一直等迟到数据 → 窗口永远关不掉，结果永远出不来，
    而且 Spark 要一直保存所有窗口的中间状态，内存会无限膨胀直到 OOM
  - 如果完全不等 → 迟到的数据就丢了，统计不准

【watermark 就是这个取舍的答案】
withWatermark("event_time", "2 minutes") 的含义：
  "我最多容忍 2 分钟的迟到。当我已经见过 20:07 的数据时，
   水位线 = 20:07 - 2分钟 = 20:05。
   凡是窗口结束时间 早于 20:05 的窗口，我认为不会再有新数据了，
   可以输出最终结果并清理它的状态。之后再来的属于这些窗口的数据，直接丢弃。"

【带来的好处】
  1. 容忍合理的乱序（2 分钟内的迟到照常计入）
  2. 窗口能及时关闭输出结果（不会永远等）
  3. 状态能及时清理（内存不会无限增长）—— 这是流作业能长期稳定运行的关键

【为什么选 2 分钟】
我们的 Producer 每 30 秒采集一次，正常延迟在秒级。
2 分钟 = 4 个采集周期的缓冲，足够覆盖网络抖动和重试，
又不会让窗口关闭太慢。这是"时效性 vs 完整性"的权衡。
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    count, approx_count_distinct, avg, round as sround, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, IntegerType
)

KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "flights"
GOLD_STREAM_PATH = "/data/delta/gold_overview_stream"
CHECKPOINT_PATH = "/data/checkpoint/gold_overview_stream"

# ---- watermark 容忍的最大迟到时间 ----
WATERMARK_DELAY = "2 minutes"
# ---- 窗口大小 ----
WINDOW_SIZE = "5 minutes"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DWS-Gold-Stream-Watermark")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def flight_schema() -> StructType:
    return StructType([
        StructField("icao24", StringType()),
        StructField("callsign", StringType()),
        StructField("origin_country", StringType()),
        StructField("time_position", LongType()),
        StructField("last_contact", LongType()),
        StructField("longitude", DoubleType()),
        StructField("latitude", DoubleType()),
        StructField("baro_altitude", DoubleType()),
        StructField("on_ground", BooleanType()),
        StructField("velocity", DoubleType()),
        StructField("true_track", DoubleType()),
        StructField("vertical_rate", DoubleType()),
        StructField("geo_altitude", DoubleType()),
        StructField("squawk", StringType()),
        StructField("spi", BooleanType()),
        StructField("position_source", IntegerType()),
        StructField("category", IntegerType()),
        StructField("snapshot_time", LongType()),
    ])


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  DWS Gold 流处理层（★ watermark 处理乱序）")
    print(f"  Kafka      : {KAFKA_TOPIC} @ {KAFKA_BOOTSTRAP}")
    print(f"  窗口大小    : {WINDOW_SIZE}")
    print(f"  watermark  : {WATERMARK_DELAY}（最多容忍这么久的迟到）")
    print(f"  写入        : {GOLD_STREAM_PATH}")
    print("=" * 60)

    # ---- 1) 从 Kafka 读流 ----
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # ---- 2) 解析 JSON ----
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), flight_schema()).alias("d"))
        .select("d.*")
        # event_time：数据"实际发生"的时间，watermark 就是基于它算的
        # （不是数据"到达"的时间，那叫 processing time）
        .withColumn("event_time", to_timestamp(col("snapshot_time")))
    )

    # ---- 3) 清洗（和 DWD 层同样的规则，流里也要做）----
    cleaned = (
        parsed
        .filter(col("on_ground") == False)
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
        .filter(col("event_time").isNotNull())
        .withColumn("speed_kmh", sround(col("velocity") * 3.6, 1))
        .withColumn("altitude_m", sround(col("baro_altitude"), 0))
        .withColumn("flight_phase", expr("""
            CASE
                WHEN vertical_rate IS NULL  THEN 'UNKNOWN'
                WHEN vertical_rate >  1.0   THEN 'CLIMBING'
                WHEN vertical_rate < -1.0   THEN 'DESCENDING'
                ELSE 'CRUISING'
            END
        """))
    )

    # ================================================================
    # ★★★ 4) watermark + 窗口聚合 —— 本项目的技术核心 ★★★
    # ================================================================
    #
    #  withWatermark 必须在 groupBy(window(...)) 之前调用，
    #  且 watermark 用的列必须就是窗口用的那个时间列（event_time）。
    #
    #  执行逻辑：
    #    - Spark 持续跟踪见过的最大 event_time（比如 20:07）
    #    - 水位线 = 最大 event_time - 2分钟 = 20:05
    #    - 结束时间 < 20:05 的窗口 → 输出最终结果 + 清理状态
    #    - 之后再来的属于已关闭窗口的数据 → 丢弃（这就是"迟到太久不要了"）
    #
    windowed = (
        cleaned
        .withWatermark("event_time", WATERMARK_DELAY)   # ← 核心这一行
        .groupBy(
            window(col("event_time"), WINDOW_SIZE)      # 5 分钟滚动窗口
        )
        .agg(
            # ★★★ 注意：流处理里不能用 countDistinct！★★★
            # Spark 会直接报错：
            #   "Distinct aggregations are not supported on streaming DataFrames"
            #
            # 【为什么？】
            # 精确去重要求记住"见过的每一个不同值"，在无界流上这个状态会
            # 无限增长直到 OOM。所以 Spark 直接禁止了这个操作。
            #
            # 【解决方案】approx_count_distinct（基于 HyperLogLog 算法）
            # 用固定大小的概率数据结构做基数估算，
            # 默认误差约 5%，状态大小恒定，可以安全地长期运行。
            #
            # 【这是个真实的流批差异】批处理版(dws_gold_batch.py)用的是精确
            # COUNT(DISTINCT)，流版必须用近似值 —— 这正是"流处理要为无界数据
            # 做取舍"的经典体现，可以主动讲这个点。
            approx_count_distinct("icao24").alias("total_flights"),
            approx_count_distinct("origin_country").alias("country_cnt"),
            count("*").alias("record_cnt"),
            sround(avg("altitude_m"), 0).alias("avg_altitude_m"),
            sround(avg("speed_kmh"), 1).alias("avg_speed_kmh"),
            # 一次扫描算三种飞行阶段（同样用近似去重）
            approx_count_distinct(
                expr("CASE WHEN flight_phase='CLIMBING'   THEN icao24 END")
            ).alias("climbing_cnt"),
            approx_count_distinct(
                expr("CASE WHEN flight_phase='DESCENDING' THEN icao24 END")
            ).alias("descending_cnt"),
            approx_count_distinct(
                expr("CASE WHEN flight_phase='CRUISING'   THEN icao24 END")
            ).alias("cruising_cnt"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "total_flights", "country_cnt", "record_cnt",
            "avg_altitude_m", "avg_speed_kmh",
            "climbing_cnt", "descending_cnt", "cruising_cnt",
        )
    )

    # ================================================================
    #  5) 输出模式的选择 —— 也是考点
    # ================================================================
    #
    #  三种 outputMode：
    #    - append   : 只输出"已确定不再变化"的结果。
    #                 有 watermark 的窗口聚合才能用 append —— 因为 watermark
    #                 让 Spark 知道哪些窗口已经关闭、结果不会再变了。
    #                 ★ 我们选它：只有 append 能写进 Delta（Delta 流式写入要求）
    #    - update   : 每个微批输出"有变化"的行（同一窗口会多次输出）
    #    - complete : 每次输出全量结果（状态无限增长，不适合长跑）
    #
    #  这也解释了 watermark 和 append 的因果关系：
    #  没有 watermark，Spark 永远不知道窗口何时"最终确定"，就没法 append。
    #
    query = (
        windowed.writeStream
        .format("delta")
        .outputMode("append")                       # ← 依赖 watermark 才能用
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="30 seconds")
        .start(GOLD_STREAM_PATH)
    )

    print("[dws-stream] 流已启动，watermark 正在处理乱序数据…")
    print("[dws-stream] 注意：窗口需等 watermark 推进才会输出，")
    print(f"[dws-stream] 首批结果大约需要 {WINDOW_SIZE} + {WATERMARK_DELAY} 后才出现，请耐心等待。")
    print("[dws-stream] 按 Ctrl+C 停止")
    query.awaitTermination()


if __name__ == "__main__":
    main()
