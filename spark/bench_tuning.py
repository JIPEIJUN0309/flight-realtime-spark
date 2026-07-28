"""
阶段八：Spark 调优 —— 同一负载，调优前 vs 调优后
=====================================================
不改逻辑，只改配置，量化每个调优技巧的效果。复用阶段七造的
flights_big（410 万行 Parquet）+ dim/airports.csv（20 行维表）。

三个实验：
  A) shuffle 分区数 & AQE   —— 聚合负载：200 分区(默认) vs 8 分区(手调) vs AQE 自动合并
  B) 广播 join (broadcast)  —— join 负载：Sort-Merge Join vs Broadcast Join
  C) 分区裁剪 (partition pruning) —— 读 silver(按 dt 分区)，看执行计划里的 PartitionFilters

计时：每个配置跑 3 轮取最快(warm)。用 collect() 触发真正执行。

跑法：
  docker exec spark /opt/spark/bin/spark-submit \
      /opt/spark/work-dir/jobs/bench_tuning.py
"""

import time
from pyspark.sql import SparkSession

FLIGHTS_BIG = "/data/bench/flights_big"
AIRPORTS_CSV = "/opt/spark/work-dir/dim/airports.csv"
SILVER = "/data/delta/silver_flights"
RUNS = 3


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Bench-Tuning")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def set_conf(spark, **kw):
    for k, v in kw.items():
        spark.conf.set(k, str(v))


def run_timed(label, make_df, runs=RUNS):
    """make_df: 无参函数，每轮重建 DataFrame（避免复用），collect 触发执行并计时。"""
    times = []
    for _ in range(runs):
        df = make_df()
        t0 = time.perf_counter()
        df.collect()
        times.append(time.perf_counter() - t0)
    best = min(times)
    print(f"    [{label}]  best={best:6.3f}s   runs={[round(t,3) for t in times]}")
    return best


def join_strategy(df) -> str:
    """从物理计划里认出 join 策略（BroadcastHashJoin / SortMergeJoin）。"""
    try:
        plan = df._jdf.queryExecution().executedPlan().toString()
        if "BroadcastHashJoin" in plan or "BroadcastNestedLoop" in plan:
            return "BroadcastHashJoin（广播）"
        if "SortMergeJoin" in plan:
            return "SortMergeJoin（走 shuffle）"
        return "其它"
    except Exception as e:
        return f"(计划解析失败: {str(e)[:40]})"


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ---- 准备源 ----
    spark.read.parquet(FLIGHTS_BIG).createOrReplaceTempView("flights_big")
    (spark.read.option("header", "true").option("inferSchema", "true")
     .csv(AIRPORTS_CSV).createOrReplaceTempView("airports"))
    big_n = spark.table("flights_big").count()
    print("=" * 66)
    print(f"  阶段八 Spark 调优基准   flights_big={big_n:,} 行  airports=20 行")
    print("=" * 66)

    # ================================================================
    # 实验 A：shuffle 分区数 & AQE（聚合负载）
    # 200 分区对这点数据是浪费（一堆小任务）；手调 8 或让 AQE 自动合并都更快。
    # ================================================================
    AGG = """
        SELECT COUNT(*) AS g, ROUND(SUM(flight_cnt), 0) AS s
        FROM (
            SELECT FLOOR(event_ts/300)*300 AS w, origin_country,
                   COUNT(DISTINCT icao24)  AS flight_cnt,
                   ROUND(AVG(altitude_m),0) AS a
            FROM flights_big
            GROUP BY FLOOR(event_ts/300)*300, origin_country
        ) t
    """
    def aggdf():
        return spark.sql(AGG)

    print("\n[A] shuffle 分区数 & AQE（聚合 410 万行）")
    set_conf(spark, **{"spark.sql.adaptive.enabled": "false",
                       "spark.sql.shuffle.partitions": "200"})
    a1 = run_timed("A1 AQE关 · 200分区(默认 naive)", aggdf)
    set_conf(spark, **{"spark.sql.adaptive.enabled": "false",
                       "spark.sql.shuffle.partitions": "8"})
    a2 = run_timed("A2 AQE关 · 8分区(手调)", aggdf)
    set_conf(spark, **{"spark.sql.adaptive.enabled": "true",
                       "spark.sql.shuffle.partitions": "200"})
    a3 = run_timed("A3 AQE开 · 自动合并分区", aggdf)

    # ================================================================
    # 实验 B：广播 join（join 负载）
    # 大表(410万) join 小维表(20行)。关广播 => 两边都 shuffle(Sort-Merge)；
    # 开广播 => 把 20 行维表广播到各 executor，大表不用 shuffle。
    # ================================================================
    JOIN = """
        SELECT a.country, COUNT(*) AS cnt, ROUND(AVG(f.altitude_m),0) AS avg_alt
        FROM flights_big f
        JOIN airports a ON f.near_airport_iata = a.iata
        GROUP BY a.country
    """
    def joindf():
        return spark.sql(JOIN)

    print("\n[B] 广播 join（410 万 × 20 维表）")
    # B1：关广播 → Sort-Merge Join
    set_conf(spark, **{"spark.sql.adaptive.enabled": "false",
                       "spark.sql.autoBroadcastJoinThreshold": "-1",
                       "spark.sql.shuffle.partitions": "200"})
    print("    B1 join 策略:", join_strategy(joindf()))
    b1 = run_timed("B1 广播关 → Sort-Merge Join", joindf)
    # B2：开广播 → Broadcast Join
    set_conf(spark, **{"spark.sql.adaptive.enabled": "false",
                       "spark.sql.autoBroadcastJoinThreshold": str(10 * 1024 * 1024)})
    print("    B2 join 策略:", join_strategy(joindf()))
    b2 = run_timed("B2 广播开 → Broadcast Join", joindf)

    # ================================================================
    # 实验 C：分区裁剪（读 silver，按 dt 分区）
    # 加 dt 过滤时，执行计划里会出现 PartitionFilters，只扫对应分区目录。
    # ================================================================
    print("\n[C] 分区裁剪（silver 按 dt 分区）")
    sdf = spark.read.format("delta").load(SILVER)
    dts = sorted([r.dt for r in sdf.select("dt").distinct().collect()])
    print(f"    silver 里的 dt 分区: {dts}")
    if dts:
        one = dts[0]
        pruned = sdf.where(sdf.dt == one)
        full_cnt = sdf.count()
        pruned_cnt = pruned.count()
        print(f"    全量行数={full_cnt:,}  |  只取 dt={one} 行数={pruned_cnt:,}")
        print("    ↓ 加 dt 过滤后的物理计划（找 PartitionFilters 那行）:")
        pruned.groupBy().count().explain()

    # ================================================================
    # 汇总：调优前后
    # ================================================================
    def pct(base, new):
        return f"省 {(base-new)/base*100:4.1f}%" if new < base else f"慢 {(new-base)/base*100:4.1f}%"
    print("\n" + "=" * 66)
    print("  调优前后汇总（把这段贴给我）")
    print("=" * 66)
    print("  [A] shuffle 分区 & AQE（聚合）:")
    print(f"      A1 200分区(基线) : {a1:6.3f}s")
    print(f"      A2 8分区(手调)   : {a2:6.3f}s   ({pct(a1,a2)}, 提速 {a1/a2:.2f}×)")
    print(f"      A3 AQE自动合并   : {a3:6.3f}s   ({pct(a1,a3)}, 提速 {a1/a3:.2f}×)")
    print("  [B] 广播 join:")
    print(f"      B1 Sort-Merge(基线): {b1:6.3f}s")
    print(f"      B2 Broadcast       : {b2:6.3f}s   ({pct(b1,b2)}, 提速 {b1/b2:.2f}×)")
    print("=" * 66)

    spark.stop()


if __name__ == "__main__":
    main()
