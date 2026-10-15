"""
采集进度体检：现在的数据够不够训练时序模型？
========================================================================
采集过程中随时可以跑，不影响正在运行的采集器和流作业。

跑法：
    docker exec -it spark /opt/spark/bin/spark-submit \
        /opt/spark/work-dir/jobs/check_collection.py

★ 为什么需要它

  时序模型要的不是"总行数大"，是【每架飞机足够长的连续航迹】。
  10 万行分散在 5000 架飞机上（每架 20 个点）切不出几个窗口，
  而 5 万行集中在 800 架飞机上（每架 60 个点）就绰绰有余。

  所以判断"够没够"的指标不是总量，是
  【有效点数 ≥ MIN_POINTS 的飞机有多少架】。
  这个脚本直接按这个口径统计 bronze。
"""

from pyspark.sql import SparkSession

BRONZE_PATH = "/data/delta/bronze_flights"

MIN_POINTS = 30        # 与 ml_common.MIN_POINTS 一致
TARGET_AIRCRAFT = 300  # 建议至少这么多架达标飞机再停止采集
GOOD_AIRCRAFT = 800    # 到这个量就很充裕了


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Check-Collection")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze = spark.read.format("delta").load(BRONZE_PATH)
    except Exception:
        print("\n  还读不到 bronze 表。可能是采集刚开始、流作业还没写出第一批。")
        print("  等几分钟再跑一次。\n")
        spark.stop()
        return

    bronze.createOrReplaceTempView("bronze_flights")

    # 按 ml_features.py 的同一口径统计：在飞 + 建模必填字段非空 + 去重
    stats = spark.sql(f"""
    WITH valid AS (
        SELECT icao24, event_time
        FROM bronze_flights
        WHERE on_ground = false
          AND latitude      IS NOT NULL
          AND longitude     IS NOT NULL
          AND baro_altitude IS NOT NULL
          AND velocity      IS NOT NULL
          AND true_track    IS NOT NULL
          AND vertical_rate IS NOT NULL
    ),
    deduped AS (
        SELECT icao24, event_time,
               ROW_NUMBER() OVER (PARTITION BY icao24, event_time
                                  ORDER BY event_time) AS rn
        FROM valid
    ),
    per_ac AS (
        SELECT icao24, COUNT(*) AS pts
        FROM deduped WHERE rn = 1
        GROUP BY icao24
    )
    SELECT
        (SELECT COUNT(*) FROM bronze_flights)                     AS raw_rows,
        (SELECT COUNT(*) FROM deduped WHERE rn = 1)               AS valid_pts,
        (SELECT COUNT(*) FROM per_ac)                             AS n_aircraft,
        (SELECT COUNT(*) FROM per_ac WHERE pts >= {MIN_POINTS})   AS n_ready,
        (SELECT ROUND(AVG(pts), 1) FROM per_ac)                   AS avg_pts,
        (SELECT MAX(pts) FROM per_ac)                             AS max_pts,
        (SELECT MIN(event_time) FROM bronze_flights)              AS t_from,
        (SELECT MAX(event_time) FROM bronze_flights)              AS t_to
    """).collect()[0]

    span_h = 0.0
    if stats["t_from"] and stats["t_to"]:
        span_h = (stats["t_to"] - stats["t_from"]).total_seconds() / 3600.0

    print("\n" + "=" * 58)
    print("  采集进度体检")
    print("=" * 58)
    print(f"  采集时间跨度      : {span_h:.1f} 小时  (UTC {stats['t_from']} → {stats['t_to']})")
    print(f"  bronze 原始行数   : {stats['raw_rows']:,}")
    print(f"  建模可用有效点    : {stats['valid_pts']:,}")
    print(f"  出现过的飞机      : {stats['n_aircraft']:,} 架")
    print(f"  平均每架点数      : {stats['avg_pts']}   (最多的一架 {stats['max_pts']} 点)")
    print("-" * 58)
    print(f"  ★ 达标飞机(≥{MIN_POINTS}点) : {stats['n_ready']:,} 架")
    print("=" * 58)

    n = stats["n_ready"]
    if n >= GOOD_AIRCRAFT:
        print(f"\n  ✅ 很充裕（≥{GOOD_AIRCRAFT}）。可以停止采集了。")
        print("     Ctrl+C 关掉采集器和流作业那两个窗口。")
    elif n >= TARGET_AIRCRAFT:
        print(f"\n  ✅ 够用了（≥{TARGET_AIRCRAFT}）。现在停也行，再采几小时会更稳。")
        print("     判断依据：训练/验证/留出按 6:2:2 分，"
              f"留出集能分到约 {n // 5} 架，足够算出可信的对照指标。")
    else:
        need_h = max(1.0, span_h * (TARGET_AIRCRAFT / max(n, 1) - 1)) if n else 6.0
        print(f"\n  ⏳ 还不够（目标 {TARGET_AIRCRAFT} 架，现在 {n} 架）。")
        print(f"     按当前速率估算，还需要大约 {need_h:.0f} 小时。")
        print("     让采集器继续跑着，过一两小时再跑一次这个脚本。")
        if span_h > 2 and stats["avg_pts"] and stats["avg_pts"] < 15:
            print("\n     ⚠️  平均每架点数偏低，说明飞机进出 bbox 太快、停留时间短。")
            print("         这是正常现象（bbox 不大），靠延长采集时间累积达标飞机即可。")

    print()
    spark.stop()


if __name__ == "__main__":
    main()
