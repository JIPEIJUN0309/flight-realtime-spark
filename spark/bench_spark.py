"""
阶段七 (2/3)：Spark 侧基准
=====================================================
读第 2 步导出的 Parquet(/data/bench/flights_small、flights_big) →
跑【和 Hive 完全同一段】聚合 SQL → 计时。

★ 公平性说明（要能讲清）
  - 两个引擎读的是同一批 Parquet 物理文件。
  - SQL 文本逐字相同（见下面 QUERY_TEMPLATE，Hive 侧 bench_hive.sql 内联同一段）。
  - 时间分桶用 FLOOR(event_ts/300)*300，两引擎语义一致。
  - Spark 引擎常驻（JVM 已热），Hive 每条查询要启动 Tez 作业 —— 这本身
    就是两种引擎的真实差异，不是作弊，我们如实记录并解释。

  为压掉噪音，每份数据跑 RUNS 次，取最快一次（best）做对比。

跑法：
  docker exec spark /opt/spark/bin/spark-submit \
      /opt/spark/work-dir/jobs/bench_spark.py
"""

import time
from pyspark.sql import SparkSession

BENCH = {
    "flights_small": "/data/bench/flights_small",
    "flights_big":   "/data/bench/flights_big",
}
RUNS = 3

# ================================================================
#  ★ 基准查询（Hive / Spark 同一段，唯一变量是表名 {table}）
#  5 分钟窗口 × 国家 聚合（含精确 COUNT(DISTINCT)）+ 外层窗口函数占比/排名。
#  外面再套一层 COUNT/SUM，逼引擎把整条链路算完、又不必打印几十万行。
# ================================================================
QUERY_TEMPLATE = """
SELECT COUNT(*) AS groups, ROUND(SUM(pct_of_window), 2) AS chk_pct, SUM(rank_in_window) AS chk_rank
FROM (
    SELECT
        window_start, origin_country, flight_cnt, record_cnt, avg_alt, avg_spd,
        climbing, descending, near_airport_cnt,
        ROUND(100.0 * flight_cnt / SUM(flight_cnt) OVER (PARTITION BY window_start), 2) AS pct_of_window,
        RANK() OVER (PARTITION BY window_start ORDER BY flight_cnt DESC) AS rank_in_window
    FROM (
        SELECT
            FLOOR(event_ts / 300) * 300 AS window_start,
            origin_country,
            COUNT(*) AS record_cnt,
            COUNT(DISTINCT icao24) AS flight_cnt,
            ROUND(AVG(altitude_m), 0) AS avg_alt,
            ROUND(AVG(speed_kmh), 1) AS avg_spd,
            SUM(CASE WHEN flight_phase = 'CLIMBING' THEN 1 ELSE 0 END) AS climbing,
            SUM(CASE WHEN flight_phase = 'DESCENDING' THEN 1 ELSE 0 END) AS descending,
            SUM(CASE WHEN near_airport THEN 1 ELSE 0 END) AS near_airport_cnt
        FROM {table}
        GROUP BY FLOOR(event_ts / 300) * 300, origin_country
    ) agg
) t
""".strip()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Bench-Spark")
        # 数据量 400 万，shuffle 分区给 8（和项目其它作业一个量级；默认 200 对这点数据是浪费）
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.adaptive.enabled", "true")   # AQE：项目里本就开着，保持真实用法
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  阶段七：Spark 侧基准（同一段 SQL，计时取最快一次）")
    print("=" * 60)

    results = {}
    for name, path in BENCH.items():
        df = spark.read.parquet(path)
        df.createOrReplaceTempView(name)
        rows = df.count()   # 预热读盘 + 拿行数
        sql = QUERY_TEMPLATE.format(table=name)

        times = []
        for i in range(RUNS):
            t0 = time.perf_counter()
            out = spark.sql(sql).collect()        # collect 触发整条 DAG 真正执行
            dt = time.perf_counter() - t0
            times.append(dt)
            print(f"[spark] {name:14s} 第{i+1}轮: {dt:6.3f}s   结果={out[0]}")

        best = min(times)
        results[name] = (rows, best, times)
        print(f"[spark] {name:14s} 行数={rows:,}  最快={best:.3f}s  "
              f"三轮={[round(x,3) for x in times]}\n")

    print("=" * 60)
    print("  Spark 侧结果汇总（把这几行贴给我）：")
    for name, (rows, best, times) in results.items():
        print(f"  {name:14s} | {rows:>10,} 行 | best {best:6.3f}s | runs {[round(x,3) for x in times]}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
