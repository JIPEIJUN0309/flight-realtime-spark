"""
阶段五：ADS（应用层）—— Delta MERGE 增量更新 + JDBC 写 PostgreSQL
====================================================================
读 gold/silver → 算对外指标 → MERGE 进 Delta ADS 表 → JDBC 写 PostgreSQL

【这一层的两个技术核心】

★ 1. Delta MERGE（UPSERT）—— 实时版 SCD Type 1 / 拉链表
   传统数仓里，"每个实体只保留最新状态"要靠拉链表 + 全量重写，很重。
   Delta 的 MERGE 让你一条语句就能做到：
       匹配上了 → UPDATE（更新为最新状态）
       没匹配上 → INSERT（新实体入表）
   而且是 ACID 的，不会读到中间状态。
   本作业的 ads_latest_position 表就是最典型的场景：
   每架飞机只保留最新位置，新数据来了就覆盖旧的。

★ 2. JDBC 写 PostgreSQL —— 分析层与服务层解耦
   Delta 适合大规模分析，但 BI 工具（Metabase）直接查 Delta 很别扭。
   所以 ADS 层把"结果指标"写进 PostgreSQL，让 BI 直接查关系库。
   这是数仓的标准做法：DWS 在湖里算，ADS 落到服务库。

【产出 5 张 ADS 表】
   1. ads_latest_position  —— 每架飞机最新位置（给地图用）★MERGE 主场
   2. ads_airport_traffic  —— 各机场流量排名
   3. ads_country_share    —— 各国航班占比
   4. ads_realtime_kpi     —— 实时总览 KPI（大屏顶部数字卡片）
   5. ads_flight_alert     —— 异常航迹告警
"""

from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable

# ---------------- 路径配置 ----------------
SILVER_PATH = "/data/delta/silver_flights"
GOLD_AIRPORT_PATH = "/data/delta/gold_airport_5min"
GOLD_COUNTRY_PATH = "/data/delta/gold_country_5min"
GOLD_OVERVIEW_PATH = "/data/delta/gold_overview_5min"

ADS_BASE = "/data/delta/ads"

# ---------------- PostgreSQL 配置 ----------------
# 注意：用容器服务名 postgres，不是 localhost
PG_URL = "jdbc:postgresql://postgres:5432/flightdb"
PG_PROPS = {
    "user": "flight",
    "password": "flight123",
    "driver": "org.postgresql.Driver",
}

# ================================================================
#  告警阈值（★ 基于航空常识校准，不是拍脑袋定的）
# ================================================================
#  单位换算参考：1 m/s = 196.85 ft/min
#
#  第一版我把爬升阈值设成 15 m/s，结果 32581 条数据里报了 394 条
#  "急剧爬升"，告警率高达 1.9%。排查后发现：
#     15 m/s = 2953 ft/min，而客机【正常】初始爬升就是 2000-3000 ft/min
#  等于我在告警"飞机起飞了"，这不是异常检测，是噪音制造。
#
#  教训：规则式检测的阈值必须基于领域知识。阈值定错，
#  告警会淹没在噪音里，运维根本不会看 —— 等于没做。
# ----------------------------------------------------------------
TH_RAPID_CLIMB   = 25      # m/s ≈ 4900 ft/min，客机罕见（正常初始爬升 10-15 m/s）
TH_RAPID_DESCENT = -30     # m/s ≈ -5900 ft/min，进入紧急下降区间（正常下降 7-13 m/s）
TH_HIGH_ALT      = 14500   # m ≈ 47,600 ft，超出公务机正常巡航（A350/B787 可到 42,000 ft）
TH_HIGH_SPEED    = 1100    # km/h，接近音速，民航地速罕见
TH_LOW_ALT       = 1500    # m，低空
TH_LOW_ALT_SPEED = 500     # km/h，低空还这么快不正常
TH_FAR_AIRPORT   = 50      # km，距最近机场超过这个距离就不该在低空高速


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ADS-Export")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def merge_into_delta(spark: SparkSession, df: DataFrame,
                     path: str, keys: list, name: str,
                     sync: bool = False):
    """
    ★ Delta MERGE 的通用封装（本作业的技术核心）

    首次运行时表不存在 → 直接写创建
    之后每次运行 → MERGE

    ================================================================
    ★★ 关键概念：MERGE 有两种语义，用错会留下脏数据 ★★
    ================================================================

    【sync=False —— UPSERT 语义（默认）】
        WHEN MATCHED     → UPDATE   （已有的，更新为最新）
        WHEN NOT MATCHED → INSERT   （新来的，插入）
        目标表里"源里没有"的行 → 原样保留，不动。

        适用：源是【增量】，目标要累积历史。
        例：ads_latest_position —— 每架飞机保留最后已知状态（SCD Type 1）。
            这批没扫到的飞机，不代表它消失了，位置该留着。

    【sync=True —— 同步/镜像语义】
        在 UPSERT 基础上，多加一个分支：
        WHEN NOT MATCHED BY SOURCE → DELETE  （目标有、源没有 → 删掉）
        结果：目标表和源【完全一致】。

        适用：源是【全量真相】，目标必须镜像它。
        例：ads_flight_alert —— 告警是按规则从 silver 全量重算的。
            规则一改，旧规则算出的告警就是垃圾，必须清掉。

    ================================================================
    【我踩过的坑（可讲）】
    第一版我只写了 MATCHED / NOT MATCHED 两个分支，以为 MERGE 会让
    目标表和源保持一致。结果把告警阈值从 15 调到 25、重算只剩 2 条告警后，
    表里【依然是 623 条】—— 因为 upsert 永远不删除，621 条旧规则的误报
    全赖在表里没走。

    教训：MERGE 默认是 upsert，不是 replace。
    要"目标 == 源"，必须显式加 whenNotMatchedBySourceDelete()。
    ================================================================
    """
    if DeltaTable.isDeltaTable(spark, path):
        cond = " AND ".join([f"t.{k} = s.{k}" for k in keys])
        builder = (
            DeltaTable.forPath(spark, path).alias("t")
            .merge(df.alias("s"), cond)
            .whenMatchedUpdateAll()      # 匹配上 → 全字段更新为最新
            .whenNotMatchedInsertAll()   # 没匹配 → 作为新记录插入
        )
        if sync:
            # 目标里有、源里没有 → 删除（让目标严格镜像源）
            builder = builder.whenNotMatchedBySourceDelete()
        builder.execute()
        mode = "MERGE + 清理陈旧行（sync）" if sync else "MERGE（UPSERT）"
        print(f"[ads] {name}: {mode} 完成")
    else:
        df.write.format("delta").mode("overwrite") \
            .option("overwriteSchema", "true").save(path)
        print(f"[ads] {name}: 首次创建 Delta 表")


def write_to_pg(df: DataFrame, table: str, mode: str = "overwrite"):
    """通过 JDBC 把结果写进 PostgreSQL，供 Metabase 直接查询。"""
    (
        df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table)
        .option("user", PG_PROPS["user"])
        .option("password", PG_PROPS["password"])
        .option("driver", PG_PROPS["driver"])
        # 批量写入大小，减少往返次数
        .option("batchsize", "1000")
        .mode(mode)
        .save()
    )
    print(f"[ads] → PostgreSQL.{table} 写入完成")


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 62)
    print("  ADS 应用层：Delta MERGE + JDBC → PostgreSQL")
    print(f"  告警阈值: 爬升>{TH_RAPID_CLIMB}m/s ({TH_RAPID_CLIMB*196.85:.0f}ft/min) | "
          f"下降<{TH_RAPID_DESCENT}m/s | 高度>{TH_HIGH_ALT}m ({TH_HIGH_ALT*3.28084:.0f}ft)")
    print("=" * 62)

    # ---- 注册源表 ----
    spark.read.format("delta").load(SILVER_PATH) \
        .createOrReplaceTempView("silver_flights")
    spark.read.format("delta").load(GOLD_AIRPORT_PATH) \
        .createOrReplaceTempView("gold_airport_5min")
    spark.read.format("delta").load(GOLD_COUNTRY_PATH) \
        .createOrReplaceTempView("gold_country_5min")
    spark.read.format("delta").load(GOLD_OVERVIEW_PATH) \
        .createOrReplaceTempView("gold_overview_5min")

    # ================================================================
    # 表1：ads_latest_position —— 每架飞机的最新位置
    #      ★★★ 这是 MERGE 的最佳示范场景 ★★★
    #      每架飞机(icao24)只保留一条最新记录：
    #        - 已存在的飞机 → UPDATE 成新位置
    #        - 新出现的飞机 → INSERT
    #      这正是"拉链表/SCD Type 1"的实时版，一条 MERGE 搞定。
    # ================================================================
    latest_pos_sql = """
    WITH ranked AS (
        SELECT
            icao24, callsign, origin_country,
            latitude, longitude,
            altitude_m, speed_kmh, heading_deg, heading_dir,
            flight_phase,
            near_airport_iata, near_airport_city, airport_dist_km,
            event_time,
            -- 每架飞机按时间倒序，取最新那条
            ROW_NUMBER() OVER (PARTITION BY icao24 ORDER BY event_time DESC) AS rn
        FROM silver_flights
    )
    SELECT
        icao24, callsign, origin_country,
        latitude, longitude,
        altitude_m, speed_kmh, heading_deg, heading_dir,
        flight_phase,
        near_airport_iata, near_airport_city, airport_dist_km,
        event_time
    FROM ranked
    WHERE rn = 1
    """
    latest_pos = spark.sql(latest_pos_sql)
    print(f"[ads] ads_latest_position: {latest_pos.count()} 架飞机（去重后最新位置）")
    merge_into_delta(spark, latest_pos, f"{ADS_BASE}/latest_position",
                     ["icao24"], "ads_latest_position")
    write_to_pg(spark.read.format("delta").load(f"{ADS_BASE}/latest_position"),
                "ads_latest_position")

    # ================================================================
    # 表2：ads_airport_traffic —— 各机场流量排名
    # ================================================================
    airport_sql = """
    SELECT
        window_start,
        near_airport_iata   AS airport_iata,
        near_airport_city   AS airport_city,
        flight_cnt,
        arriving_cnt,
        departing_cnt,
        avg_altitude_m,
        avg_speed_kmh,
        RANK() OVER (PARTITION BY window_start ORDER BY flight_cnt DESC)
                            AS rank_in_window
    FROM gold_airport_5min
    """
    airport_traffic = spark.sql(airport_sql)
    print(f"[ads] ads_airport_traffic: {airport_traffic.count()} 条")
    merge_into_delta(spark, airport_traffic, f"{ADS_BASE}/airport_traffic",
                     ["window_start", "airport_iata"], "ads_airport_traffic")
    write_to_pg(spark.read.format("delta").load(f"{ADS_BASE}/airport_traffic"),
                "ads_airport_traffic")

    # ================================================================
    # 表3：ads_country_share —— 各国航班占比
    # ================================================================
    country_sql = """
    SELECT
        window_start, origin_country,
        flight_cnt, avg_altitude_m, avg_speed_kmh,
        CAST(pct_of_window AS DOUBLE) AS pct_of_window,
        rank_in_window
    FROM gold_country_5min
    """
    country_share = spark.sql(country_sql)
    print(f"[ads] ads_country_share: {country_share.count()} 条")
    merge_into_delta(spark, country_share, f"{ADS_BASE}/country_share",
                     ["window_start", "origin_country"], "ads_country_share")
    write_to_pg(spark.read.format("delta").load(f"{ADS_BASE}/country_share"),
                "ads_country_share")

    # ================================================================
    # 表4：ads_realtime_kpi —— 实时总览 KPI（大屏顶部数字卡片）
    # ================================================================
    kpi_sql = """
    SELECT
        window_start, window_end,
        total_flights, country_cnt,
        avg_altitude_m, avg_speed_kmh,
        max_speed_kmh, max_altitude_m,
        climbing_cnt, descending_cnt, cruising_cnt,
        near_airport_cnt,
        -- 派生：起降活跃度（爬升+下降占总数比例，反映机场繁忙程度）
        ROUND(100.0 * (climbing_cnt + descending_cnt) / total_flights, 1)
                                        AS takeoff_landing_pct
    FROM gold_overview_5min
    """
    kpi = spark.sql(kpi_sql)
    print(f"[ads] ads_realtime_kpi: {kpi.count()} 条")
    merge_into_delta(spark, kpi, f"{ADS_BASE}/realtime_kpi",
                     ["window_start"], "ads_realtime_kpi")
    write_to_pg(spark.read.format("delta").load(f"{ADS_BASE}/realtime_kpi"),
                "ads_realtime_kpi")

    # ================================================================
    # 表5：ads_flight_alert —— 异常航迹告警
    #      规则式异常检测，全部用 Spark SQL 表达
    # ================================================================
    alert_sql = f"""
    WITH tagged AS (
        SELECT
            icao24, callsign, origin_country,
            latitude, longitude, altitude_m, speed_kmh, vertical_rate,
            near_airport_city, airport_dist_km, event_time,
            CASE
                WHEN vertical_rate > {TH_RAPID_CLIMB}   THEN 'RAPID_CLIMB'
                WHEN vertical_rate < {TH_RAPID_DESCENT} THEN 'RAPID_DESCENT'
                WHEN altitude_m    > {TH_HIGH_ALT}      THEN 'UNUSUAL_ALTITUDE'
                WHEN speed_kmh     > {TH_HIGH_SPEED}    THEN 'UNUSUAL_SPEED'
                WHEN altitude_m    < {TH_LOW_ALT}
                     AND speed_kmh > {TH_LOW_ALT_SPEED}
                     AND airport_dist_km > {TH_FAR_AIRPORT}
                                                        THEN 'LOW_ALT_FAR_FROM_AIRPORT'
                ELSE NULL
            END AS alert_type,
            CASE
                WHEN vertical_rate > {TH_RAPID_CLIMB} THEN
                    CONCAT('急剧爬升 ', ROUND(vertical_rate, 1), ' m/s')
                WHEN vertical_rate < {TH_RAPID_DESCENT} THEN
                    CONCAT('急剧下降 ', ROUND(vertical_rate, 1), ' m/s')
                WHEN altitude_m > {TH_HIGH_ALT} THEN
                    CONCAT('异常高度 ', CAST(ROUND(altitude_m, 0) AS INT), ' m')
                WHEN speed_kmh > {TH_HIGH_SPEED} THEN
                    CONCAT('异常速度 ', ROUND(speed_kmh, 1), ' km/h')
                WHEN altitude_m < {TH_LOW_ALT}
                     AND speed_kmh > {TH_LOW_ALT_SPEED}
                     AND airport_dist_km > {TH_FAR_AIRPORT} THEN
                    CONCAT('低空高速且远离机场 ',
                           CAST(ROUND(airport_dist_km, 0) AS INT), ' km')
                ELSE NULL
            END AS alert_detail
        FROM silver_flights
    ),
    deduped AS (
        -- ★ 必须去重！MERGE 的源表如果对同一个目标行有多条匹配，
        --   Delta 会直接报错：
        --   "Cannot perform Merge as multiple source rows matched..."
        --   silver 里可能存在重复（例如 ODS 流用新 checkpoint 重跑过、
        --   重复消费了 Kafka），所以这里按 MERGE 键先去重，保证幂等。
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY icao24, event_time, alert_type
                ORDER BY event_time
            ) AS rn
        FROM tagged
        WHERE alert_type IS NOT NULL
    )
    SELECT
        icao24, callsign, origin_country,
        latitude, longitude,
        altitude_m, speed_kmh, vertical_rate,
        near_airport_city, airport_dist_km,
        event_time, alert_type, alert_detail
    FROM deduped
    WHERE rn = 1
    """
    alerts = spark.sql(alert_sql)
    alert_cnt = alerts.count()
    print(f"[ads] ads_flight_alert: {alert_cnt} 条告警")
    # ★ sync=True：告警是按规则从 silver 全量重算的，源就是全量真相。
    #   规则一改（比如阈值调整），旧规则算出的告警必须清掉，
    #   否则表里会留着一堆按旧标准误报的脏数据。
    merge_into_delta(spark, alerts, f"{ADS_BASE}/flight_alert",
                     ["icao24", "event_time", "alert_type"], "ads_flight_alert",
                     sync=True)
    write_to_pg(spark.read.format("delta").load(f"{ADS_BASE}/flight_alert"),
                "ads_flight_alert")

    # ================================================================
    # 打印样例，直观看效果
    # ================================================================
    print("\n[ads] 最新位置样例（给地图用）:")
    spark.read.format("delta").load(f"{ADS_BASE}/latest_position") \
        .select("callsign", "origin_country", "latitude", "longitude",
                "altitude_m", "speed_kmh", "flight_phase", "near_airport_city") \
        .show(8, truncate=False)

    print("[ads] 告警类型分布:")
    alert_tbl = spark.read.format("delta").load(f"{ADS_BASE}/flight_alert")
    tbl_cnt = alert_tbl.count()

    # ★ 数据质量校验：sync 语义下，表里的行数必须等于本次算出的行数。
    #   当初就是没这个校验，才让 621 条旧规则的误报在表里赖了一版没被发现。
    if tbl_cnt != alert_cnt:
        print(f"      ⚠️  不一致！本次算出 {alert_cnt} 条，但表里有 {tbl_cnt} 条")
        print(f"          说明 sync 没生效，表里残留了陈旧数据。")
    else:
        print(f"      ✓ 一致性校验通过：算出 {alert_cnt} 条，表里 {tbl_cnt} 条")

    if tbl_cnt > 0:
        alert_tbl.groupBy("alert_type").count() \
            .orderBy("count", ascending=False).show(truncate=False)
        print("[ads] 告警样例:")
        alert_tbl.select("callsign", "origin_country", "alert_type",
                         "alert_detail", "near_airport_city") \
            .show(5, truncate=False)
    else:
        print("      （本批数据无异常，说明航班都很正常）")

    print("\n" + "=" * 62)
    print("  ADS 层完成。5 张表已 MERGE 进 Delta 并写入 PostgreSQL。")
    print("  下一步：Metabase 连 PostgreSQL 出图（阶段六）")
    print("=" * 62)

    spark.stop()


if __name__ == "__main__":
    main()
