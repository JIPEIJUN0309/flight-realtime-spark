"""
阶段四(1/2)：DWS（Gold）层 —— 批处理版
==========================================
读 silver 明细 → Spark SQL 窗口聚合 → 写 gold 指标表

为什么先做批版？
  - 先用存量数据把聚合 SQL 的逻辑调对，比在流里调试快得多
  - 阶段七的 Hive vs Spark 性能对比需要批模式来公平计时
  - 真实开发也是这个顺序：批里验证逻辑 → 再搬到流

产出三张 gold 表：
  1. gold_airport_5min  —— 各机场 5 分钟窗口航班密度
  2. gold_country_5min  —— 各国 5 分钟窗口航班占比
  3. gold_overview_5min —— 全区域 5 分钟窗口总览指标
"""

from pyspark.sql import SparkSession

SILVER_PATH = "/data/delta/silver_flights"
GOLD_AIRPORT_PATH = "/data/delta/gold_airport_5min"
GOLD_COUNTRY_PATH = "/data/delta/gold_country_5min"
GOLD_OVERVIEW_PATH = "/data/delta/gold_overview_5min"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DWS-Gold-Batch")
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

    print("=" * 60)
    print("  DWS Gold 层（批处理版）：Spark SQL 窗口聚合")
    print("=" * 60)

    silver = spark.read.format("delta").load(SILVER_PATH)
    silver.createOrReplaceTempView("silver_flights")
    print(f"[dws] silver 明细记录数: {silver.count()}")

    # ================================================================
    # 表1：各机场 5 分钟窗口航班密度
    # 用 window() 函数把连续时间切成 5 分钟一个的桶
    # ================================================================
    airport_sql = """
    SELECT
        window(event_time, '5 minutes').start   AS window_start,
        window(event_time, '5 minutes').end     AS window_end,
        near_airport_iata,
        near_airport_city,

        -- 核心指标
        COUNT(DISTINCT icao24)                  AS flight_cnt,       -- 去重飞机数
        COUNT(*)                                AS record_cnt,       -- 原始记录数
        ROUND(AVG(altitude_m), 0)               AS avg_altitude_m,
        ROUND(AVG(speed_kmh), 1)                AS avg_speed_kmh,

        -- 机场空域内的飞机数（距机场 50km 内）
        COUNT(DISTINCT CASE WHEN near_airport THEN icao24 END)
                                                AS near_airport_cnt,

        -- 进近/离场判断：正在下降且靠近机场 = 大概率在降落
        COUNT(DISTINCT CASE WHEN flight_phase = 'DESCENDING' AND near_airport
                            THEN icao24 END)    AS arriving_cnt,
        COUNT(DISTINCT CASE WHEN flight_phase = 'CLIMBING' AND near_airport
                            THEN icao24 END)    AS departing_cnt,

        DATE(window(event_time, '5 minutes').start) AS dt

    FROM silver_flights
    GROUP BY
        window(event_time, '5 minutes'),
        near_airport_iata,
        near_airport_city
    """
    airport_agg = spark.sql(airport_sql)
    (airport_agg.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").partitionBy("dt")
     .save(GOLD_AIRPORT_PATH))
    print(f"[dws] gold_airport_5min : {airport_agg.count()} 条")

    # ================================================================
    # 表2：各国 5 分钟窗口航班占比
    # 用窗口函数算占比和排名
    # ================================================================
    country_sql = """
    WITH country_base AS (
        SELECT
            window(event_time, '5 minutes').start AS window_start,
            window(event_time, '5 minutes').end   AS window_end,
            origin_country,
            COUNT(DISTINCT icao24)                AS flight_cnt,
            ROUND(AVG(altitude_m), 0)             AS avg_altitude_m,
            ROUND(AVG(speed_kmh), 1)              AS avg_speed_kmh
        FROM silver_flights
        GROUP BY window(event_time, '5 minutes'), origin_country
    )
    SELECT
        window_start, window_end, origin_country,
        flight_cnt, avg_altitude_m, avg_speed_kmh,

        -- 窗口函数①：本国航班占该时间窗口总数的百分比
        ROUND(100.0 * flight_cnt
              / SUM(flight_cnt) OVER (PARTITION BY window_start), 2)
                                                  AS pct_of_window,

        -- 窗口函数②：同一时间窗口内按航班数排名
        RANK() OVER (PARTITION BY window_start ORDER BY flight_cnt DESC)
                                                  AS rank_in_window,

        DATE(window_start)                        AS dt
    FROM country_base
    """
    country_agg = spark.sql(country_sql)
    (country_agg.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").partitionBy("dt")
     .save(GOLD_COUNTRY_PATH))
    print(f"[dws] gold_country_5min : {country_agg.count()} 条")

    # ================================================================
    # 表3：全区域 5 分钟总览（喂大屏用的核心指标）
    # ================================================================
    overview_sql = """
    SELECT
        window(event_time, '5 minutes').start   AS window_start,
        window(event_time, '5 minutes').end     AS window_end,

        COUNT(DISTINCT icao24)                  AS total_flights,
        COUNT(DISTINCT origin_country)          AS country_cnt,
        ROUND(AVG(altitude_m), 0)               AS avg_altitude_m,
        ROUND(AVG(speed_kmh), 1)                AS avg_speed_kmh,
        ROUND(MAX(speed_kmh), 1)                AS max_speed_kmh,
        ROUND(MAX(altitude_m), 0)               AS max_altitude_m,

        -- 条件聚合：一次扫描算出多个条件计数
        COUNT(DISTINCT CASE WHEN flight_phase='CLIMBING'   THEN icao24 END) AS climbing_cnt,
        COUNT(DISTINCT CASE WHEN flight_phase='DESCENDING' THEN icao24 END) AS descending_cnt,
        COUNT(DISTINCT CASE WHEN flight_phase='CRUISING'   THEN icao24 END) AS cruising_cnt,
        COUNT(DISTINCT CASE WHEN near_airport               THEN icao24 END) AS near_airport_cnt,

        DATE(window(event_time, '5 minutes').start) AS dt

    FROM silver_flights
    GROUP BY window(event_time, '5 minutes')
    """
    overview_agg = spark.sql(overview_sql)
    (overview_agg.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").partitionBy("dt")
     .save(GOLD_OVERVIEW_PATH))
    print(f"[dws] gold_overview_5min: {overview_agg.count()} 条")

    # ---- 打印样例，直观看效果 ----
    print("\n[dws] 各机场 5 分钟密度（Top 10）:")
    spark.read.format("delta").load(GOLD_AIRPORT_PATH) \
        .select("window_start", "near_airport_city", "flight_cnt",
                "arriving_cnt", "departing_cnt", "avg_altitude_m") \
        .orderBy("flight_cnt", ascending=False).show(10, truncate=False)

    print("[dws] 各国占比（某个窗口的 Top 5）:")
    spark.read.format("delta").load(GOLD_COUNTRY_PATH) \
        .select("window_start", "origin_country", "flight_cnt",
                "pct_of_window", "rank_in_window") \
        .filter("rank_in_window <= 5").orderBy("window_start", "rank_in_window") \
        .show(10, truncate=False)

    print("[dws] 全区域总览（最近 5 个窗口）:")
    spark.read.format("delta").load(GOLD_OVERVIEW_PATH) \
        .select("window_start", "total_flights", "country_cnt",
                "climbing_cnt", "descending_cnt", "cruising_cnt") \
        .orderBy("window_start", ascending=False).show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
