"""
阶段二核心：ODS（Bronze）层
============================
用 Spark Structured Streaming 从 Kafka 持续消费航班数据，
原样落进 Delta Lake 的 bronze 表（不做加工，只做接入）。

这是整条 Spark 链路的第一环，对应"后厨开第一把火"。
ODS 层的原则：忠实保留原始数据 + Kafka 元数据，保证可追溯、可回放、可重算。
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, LongType, IntegerType
)

# ---------------- 配置 ----------------
# 容器内部用服务名 kafka:29092 互联（不是 localhost:9092，那是给宿主机用的）
KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "flights"

# Delta 表和 checkpoint 都存到挂载出来的目录，宿主机能看到、重启不丢
BRONZE_PATH = "/data/delta/bronze_flights"
CHECKPOINT_PATH = "/data/checkpoint/bronze_flights"


def build_spark() -> SparkSession:
    """创建启用 Delta Lake 的 SparkSession。"""
    return (
        SparkSession.builder
        .appName("ODS-Bronze-Flights")
        # 启用 Delta：这两行是 Delta Lake 的标准配置
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # 单机跑，shuffle 分区数调小，避免小数据量下产生过多小任务
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def flight_schema() -> StructType:
    """
    Producer 写进 Kafka 的 JSON 结构。
    字段和 opensky_producer.py 里 parse_state 产出的完全对应。
    """
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
    spark.sparkContext.setLogLevel("WARN")  # 少打点日志，看得清

    print("=" * 56)
    print("  ODS Bronze 层启动：Kafka → Delta Lake")
    print(f"  读取 Kafka : {KAFKA_TOPIC} @ {KAFKA_BOOTSTRAP}")
    print(f"  写入 Delta : {BRONZE_PATH}")
    print("=" * 56)

    # ---- 1) 从 Kafka 读流 ----
    # startingOffsets=earliest：从最早的消息开始读，把 Kafka 里已积压的也收进来
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    # ---- 2) 解析 JSON ----
    # Kafka 的 value 是二进制，先转成字符串，再按 schema 解析成结构化列
    schema = flight_schema()
    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str",
                    "topic", "partition", "offset", "timestamp AS kafka_ts")
        .select(
            from_json(col("json_str"), schema).alias("data"),
            col("topic"), col("partition"), col("offset"), col("kafka_ts")
        )
        .select("data.*", "topic", "partition", "offset", "kafka_ts")
    )

    # ---- 3) ODS 层加工：只加元数据，不动业务字段 ----
    bronze = (
        parsed
        # 把 Unix 秒时间戳转成真正的 timestamp，方便下游用
        .withColumn("event_time", to_timestamp(col("snapshot_time")))
        # 记录本条数据的入湖时间（数据血缘/审计用）
        .withColumn("ingest_time", current_timestamp())
    )

    # ---- 4) 写进 Delta Lake（append 模式，持续追加）----
    query = (
        bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        # 每 30 秒触发一次微批（和 Producer 的采集节奏对齐）
        .trigger(processingTime="30 seconds")
        .start(BRONZE_PATH)
    )

    print("[ods] 流已启动，正在持续消费 Kafka 并写入 Delta bronze 表…")
    print("[ods] 按 Ctrl+C 停止")
    query.awaitTermination()


if __name__ == "__main__":
    main()
