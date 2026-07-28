"""
阶段七 (1/3)：造公共对比数据集
=====================================================
读 silver(Delta) → 选对比要用的列 → 导出成 Parquet(两引擎中立格式)。

产出两份，都写到 /data/bench，Hive 和 Spark 读的是【同一批物理文件】：
  - 小(真实)：/data/bench/flights_small   ≈ 13.6 万行（你采集的真实样本）
  - 大(放大)：/data/bench/flights_big     ≈ MULT × 13.6 万行（默认 ×30 ≈ 410 万）

★ 为什么用 Parquet，不用 Delta？
  Hive 读不了 Delta。Parquet 是 Hive / Spark 都原生支持的列式格式，
  两个引擎读同一份 Parquet，性能对比才公平（只比引擎，不比格式差异）。

★ 放大不是"白复制 N 遍"（那样引擎会偷懒、测不出真实力）：
  每复制一份就给 icao24 加后缀、event_ts 平移 5 分钟的倍数，
  => 产生真正更多的飞机(分组)和时间窗口，聚合计算量随行数线性上涨。

★ 时间字段存成 event_ts（BIGINT，Unix 秒），不存 timestamp：
  避免 Spark/Hive 对 Parquet 时间戳类型(INT96 vs int64)和时区解释的差异，
  两引擎对同一个 BIGINT 做 floor(event_ts/300) 分桶，结果 100% 一致。

跑法（和其它 Spark 作业一样）：
  docker exec spark /opt/spark/bin/spark-submit \
      /opt/spark/work-dir/jobs/bench_prepare.py [放大倍数]
"""

import sys
from pyspark.sql import SparkSession, functions as F

SILVER = "/data/delta/silver_flights"
OUT_SMALL = "/data/bench/flights_small"
OUT_BIG = "/data/bench/flights_big"

# 放大倍数：默认 30（≈410 万行）。想换量级就传参，例如 spark-submit ... bench_prepare.py 50
MULT = int(sys.argv[1]) if len(sys.argv) > 1 else 30


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Bench-Prepare")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  阶段七：造公共对比数据集（Parquet，Hive/Spark 共读）")
    print(f"  放大倍数 MULT = {MULT}")
    print("=" * 60)

    # ---- 选出对比要用的列；event_time 转成 Unix 秒(BIGINT) ----
    base = (
        spark.read.format("delta").load(SILVER)
        .select(
            F.col("icao24"),
            F.col("origin_country"),
            F.col("event_time").cast("long").alias("event_ts"),   # Unix 秒
            F.col("altitude_m"),
            F.col("speed_kmh"),
            F.col("near_airport_iata"),
            F.col("near_airport_city"),
            F.col("near_airport"),
            F.col("flight_phase"),
        )
        # silver 可能有重复（ODS 流重跑过），先去重，保证基数干净
        .dropDuplicates(["icao24", "event_ts"])
    )
    n = base.count()
    print(f"[bench] silver 去重后真实行数: {n:,}")

    # ---- 小份：真实数据，导 Parquet ----
    base.repartition(2).write.mode("overwrite").parquet(OUT_SMALL)
    print(f"[bench] 小份已写: {OUT_SMALL}  ({n:,} 行)")

    # ---- 大份：放大 MULT 倍 ----
    reps = spark.range(0, MULT).withColumnRenamed("id", "rep")
    big = (
        base.crossJoin(reps)
        # 每份给 icao24 加后缀 => 更多不同飞机（更多分组、更大 DISTINCT 基数）
        .withColumn("icao24", F.concat_ws("_", F.col("icao24"), F.col("rep")))
        # event_ts 按 rep 平移 5 分钟的倍数 => 铺开到更多时间窗口
        .withColumn("event_ts", F.col("event_ts") + F.col("rep") * F.lit(300))
        .drop("rep")
    )
    big.repartition(8).write.mode("overwrite").parquet(OUT_BIG)
    big_n = spark.read.parquet(OUT_BIG).count()
    print(f"[bench] 大份已写: {OUT_BIG}  ({big_n:,} 行, ×{MULT})")

    print("\n[bench] 样例（大份前 5 行）：")
    spark.read.parquet(OUT_BIG).show(5, truncate=False)

    print("=" * 60)
    print("  数据就绪。下一步：两个引擎各建外部表指向这两份 Parquet。")
    print(f"  小份 {n:,} 行 | 大份 {big_n:,} 行")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
