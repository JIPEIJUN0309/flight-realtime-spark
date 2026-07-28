"""
阶段三核心：DWD（Silver）层 —— Spark SQL 清洗与维度关联
========================================================
读 bronze 原始数据 → 用 Spark SQL 清洗加工 → 写 silver 明细表

这一层是整个项目"Spark SQL 能力"的主要体现，做四件事：
  1. 清洗过滤：剔除地面飞机、无效坐标、异常值
  2. 单位换算：m/s → km/h，米 → 英尺
  3. 派生字段：按航向算方位、按垂直速率判断爬升/下降
  4. 维度关联：用经纬度就近关联机场（Haversine 距离计算）

注意：本作业是【批处理】模式（读 bronze 存量数据一次性加工）。
之所以用批而不是流，是因为：
  - 调试期用存量数据反复跑，逻辑更好验证
  - 后面阶段七的 Hive vs Spark 性能对比，也需要批模式来公平计时
  - 生产环境里 DWD 层用批（T+1）或流都有，取决于时效要求
"""

from pyspark.sql import SparkSession

# ---------------- 路径配置 ----------------
BRONZE_PATH = "/data/delta/bronze_flights"
SILVER_PATH = "/data/delta/silver_flights"
AIRPORTS_CSV = "/opt/spark/work-dir/dim/airports.csv"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DWD-Silver-Flights")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        # 开启 AQE（自适应查询执行）—— 批处理里可用，阶段八调优会重点讲
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  DWD Silver 层：Spark SQL 清洗 + 维度关联")
    print(f"  读取 bronze : {BRONZE_PATH}")
    print(f"  机场维表     : {AIRPORTS_CSV}")
    print(f"  写入 silver : {SILVER_PATH}")
    print("=" * 60)

    # ---- 1) 注册 bronze 表为临时视图，供 Spark SQL 查询 ----
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    bronze.createOrReplaceTempView("bronze_flights")
    print(f"[dwd] bronze 原始记录数: {bronze.count()}")

    # ---- 2) 加载机场维表 ----
    airports = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(AIRPORTS_CSV)
    )
    airports.createOrReplaceTempView("dim_airports")
    print(f"[dwd] 机场维表记录数: {airports.count()}")

    # ================================================================
    #  3) 核心清洗逻辑 —— 全部用 Spark SQL 写
    # ================================================================
    clean_sql = """
    SELECT
        icao24,
        -- 呼号去空格，空值补默认标记
        COALESCE(NULLIF(TRIM(callsign), ''), 'UNKNOWN')  AS callsign,
        origin_country,
        latitude,
        longitude,

        -- 单位换算：OpenSky 的 velocity 是 m/s，换算成更直观的 km/h
        ROUND(velocity * 3.6, 1)                          AS speed_kmh,
        -- 高度：保留米，同时给出英尺（航空业惯用英尺）
        ROUND(baro_altitude, 0)                           AS altitude_m,
        ROUND(baro_altitude * 3.28084, 0)                 AS altitude_ft,

        true_track                                        AS heading_deg,

        -- 派生字段①：按航向角算八方位（0=北，顺时针）
        CASE
            WHEN true_track IS NULL                        THEN 'UNKNOWN'
            WHEN true_track >= 337.5 OR true_track < 22.5  THEN 'N'
            WHEN true_track <  67.5                        THEN 'NE'
            WHEN true_track < 112.5                        THEN 'E'
            WHEN true_track < 157.5                        THEN 'SE'
            WHEN true_track < 202.5                        THEN 'S'
            WHEN true_track < 247.5                        THEN 'SW'
            WHEN true_track < 292.5                        THEN 'W'
            ELSE 'NW'
        END                                               AS heading_dir,

        -- 派生字段②：按垂直速率判断飞行阶段
        vertical_rate,
        CASE
            WHEN vertical_rate IS NULL       THEN 'UNKNOWN'
            WHEN vertical_rate >  1.0        THEN 'CLIMBING'    -- 爬升
            WHEN vertical_rate < -1.0        THEN 'DESCENDING'  -- 下降
            ELSE 'CRUISING'                                     -- 平飞
        END                                               AS flight_phase,

        event_time,
        ingest_time,
        -- 按天分区（数仓惯例，便于后续分区裁剪）
        DATE(event_time)                                  AS dt

    FROM bronze_flights
    WHERE
        -- 清洗规则①：只要在飞的，剔除地面飞机
        on_ground = false
        -- 清洗规则②：位置必须有效（没坐标的记录对我们没用）
        AND latitude  IS NOT NULL
        AND longitude IS NOT NULL
        -- 清洗规则③：坐标合法性校验（防脏数据）
        AND latitude  BETWEEN -90  AND 90
        AND longitude BETWEEN -180 AND 180
        -- 清洗规则④：剔除异常高度（民航巡航一般不超过 15000m）
        AND (baro_altitude IS NULL OR baro_altitude BETWEEN -500 AND 15000)
        -- 清洗规则⑤：剔除异常速度（超过 1400 km/h 基本是脏数据）
        AND (velocity IS NULL OR velocity BETWEEN 0 AND 400)
    """

    cleaned = spark.sql(clean_sql)
    cleaned.createOrReplaceTempView("cleaned_flights")
    cleaned_cnt = cleaned.count()
    print(f"[dwd] 清洗后记录数: {cleaned_cnt}")

    # ================================================================
    #  4) 维度关联 —— 用 Haversine 公式算球面距离，就近关联机场
    #     这是一个"非等值 join"，比普通 join 更能体现 SQL 功力
    # ================================================================
    join_sql = """
    WITH flight_airport_dist AS (
        SELECT
            f.*,
            a.iata          AS near_airport_iata,
            a.airport_name  AS near_airport_name,
            a.city          AS near_airport_city,

            -- Haversine 公式：算地球表面两点间的大圆距离(km)
            -- 6371 是地球平均半径(km)
            ROUND(
                6371 * 2 * ASIN(SQRT(
                    POWER(SIN(RADIANS(a.latitude - f.latitude) / 2), 2)
                    + COS(RADIANS(f.latitude)) * COS(RADIANS(a.latitude))
                    * POWER(SIN(RADIANS(a.longitude - f.longitude) / 2), 2)
                )), 1
            )               AS airport_dist_km,

            -- 窗口函数：给每架飞机按距离排序，取最近的那个机场
            ROW_NUMBER() OVER (
                PARTITION BY f.icao24, f.event_time
                ORDER BY
                    6371 * 2 * ASIN(SQRT(
                        POWER(SIN(RADIANS(a.latitude - f.latitude) / 2), 2)
                        + COS(RADIANS(f.latitude)) * COS(RADIANS(a.latitude))
                        * POWER(SIN(RADIANS(a.longitude - f.longitude) / 2), 2)
                    ))
            )               AS rn

        FROM cleaned_flights f
        -- 交叉关联所有机场，再取最近的（机场维表很小，广播 join 很快）
        CROSS JOIN dim_airports a
    )
    SELECT
        icao24, callsign, origin_country,
        latitude, longitude,
        speed_kmh, altitude_m, altitude_ft,
        heading_deg, heading_dir,
        vertical_rate, flight_phase,
        near_airport_iata, near_airport_name, near_airport_city,
        airport_dist_km,
        -- 派生字段③：距最近机场 50km 内视为"机场空域"
        CASE WHEN airport_dist_km <= 50 THEN true ELSE false END AS near_airport,
        event_time, ingest_time, dt
    FROM flight_airport_dist
    WHERE rn = 1
    """

    silver = spark.sql(join_sql)

    # ---- 5) 写入 silver 表（按天分区）----
    (
        silver.write
        .format("delta")
        .mode("overwrite")             # 批处理重跑时覆盖
        .option("overwriteSchema", "true")
        .partitionBy("dt")             # 按天分区，便于分区裁剪
        .save(SILVER_PATH)
    )

    silver_cnt = spark.read.format("delta").load(SILVER_PATH).count()
    print(f"[dwd] 已写入 silver 表: {silver_cnt} 条")

    # ---- 6) 打印几条样例，直观看看加工效果 ----
    print("\n[dwd] Silver 表样例（前 10 条）:")
    spark.read.format("delta").load(SILVER_PATH).select(
        "callsign", "origin_country", "speed_kmh", "altitude_m",
        "heading_dir", "flight_phase", "near_airport_iata", "airport_dist_km"
    ).show(10, truncate=False)

    # ---- 7) 数据质量报告：清洗掉了多少、为什么 ----
    print("[dwd] 数据质量报告:")
    bronze_cnt = bronze.count()
    print(f"      bronze 原始      : {bronze_cnt:>8}")
    print(f"      清洗后           : {cleaned_cnt:>8}  "
          f"(过滤掉 {bronze_cnt - cleaned_cnt}, "
          f"占 {(bronze_cnt - cleaned_cnt) / bronze_cnt * 100:.1f}%)")
    print(f"      silver 最终      : {silver_cnt:>8}")

    spark.stop()


if __name__ == "__main__":
    main()
