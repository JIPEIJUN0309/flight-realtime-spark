"""
阶段九(2/5)：从状态明细抽取建模用的航迹序列
========================================================================
读 dwd_flight_state 的空中记录 → 按飞机做去重与连续性筛选 → 写 Parquet 供建模作业使用

跑法：
    docker exec -it spark /opt/spark/bin/spark-submit \
        /opt/spark/work-dir/jobs/ml_features.py

★ 这一步为什么必须有

  状态明细是"每条上报一行"的明细，而时序模型要的是"每架飞机一条连续序列"。
  中间隔着三件脏活，全都适合用 Spark SQL 做：

    1. 去重。ODS 流用新 checkpoint 重跑过就会重复消费 Kafka，
       dwd_flight_state 里同一个 (icao24, event_time) 可能有多行。
       序列里混进重复点，采样间隔会算成 0，派生特征直接爆。
       （ads_export.py 里的告警表也踩过同一个坑，那里是 MERGE 报错，
         这里是静默地污染特征，更难发现。）

    2. 连续性筛选。只在采集窗口里露过几面的飞机切不出窗口，
       留着只会让下游反复判空。

    3. 必填字段校验。模型的 8 个特征全部依赖 altitude / speed /
       heading / vertical_rate，任一为空这个点就没法用。

  M4 起直接读 dwd_flight_state（先过滤 on_ground = false），不再读过渡期的
  silver_flights —— 那张表已经没有下游了。

  派生特征不在这里算 —— 原因见 ml_common.py 顶部的注释（要和
  ml_eval 注入合成异常后的重新派生共用同一份代码）。
"""

from pyspark.sql import SparkSession

DWD_STATE_PATH = "/data/delta/dwd_flight_state"
POINTS_PATH = "/data/ml/points"

MIN_POINTS = 30      # 与 ml_common.MIN_POINTS 保持一致


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ML-Features-Extract")
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

    print("=" * 62)
    print("  异常检测 · 特征抽取：状态明细 → 航迹序列")
    print(f"  读取 : {DWD_STATE_PATH}（只用空中记录）")
    print(f"  写入 : {POINTS_PATH}")
    print("=" * 62)

    states = (spark.read.format("delta").load(DWD_STATE_PATH)
              .where("NOT on_ground"))
    states.createOrReplaceTempView("dwd_flight_state")
    raw_cnt = states.count()
    print(f"[ml] 状态明细里的空中记录数: {raw_cnt}")

    points_sql = f"""
    WITH valid AS (
        SELECT
            icao24, event_time,
            latitude, longitude,
            altitude_m, speed_kmh,
            heading_deg, vertical_rate
        FROM dwd_flight_state
        WHERE
            -- 模型的 8 个特征全部依赖这几列，缺一个这个点就用不了
            altitude_m    IS NOT NULL
            AND speed_kmh IS NOT NULL
            AND heading_deg   IS NOT NULL
            AND vertical_rate IS NOT NULL
            AND latitude  IS NOT NULL
            AND longitude IS NOT NULL
    ),
    deduped AS (
        -- ★ 按 (icao24, event_time) 去重。
        --   重复点会让采样间隔算成 0，加速度/转弯率变成除零。
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY icao24, event_time
                ORDER BY altitude_m
            ) AS rn
        FROM valid
    ),
    counted AS (
        -- 每架飞机有多少个有效点。切窗口至少要 SEQ_LEN+1 个，
        -- 这里用更保守的 MIN_POINTS，保证每架能切出若干个窗口。
        SELECT *,
            COUNT(*) OVER (PARTITION BY icao24) AS point_cnt
        FROM deduped
        WHERE rn = 1
    )
    SELECT
        icao24, event_time,
        latitude, longitude,
        altitude_m, speed_kmh,
        heading_deg, vertical_rate
    FROM counted
    WHERE point_cnt >= {MIN_POINTS}
    """

    points = spark.sql(points_sql)
    # 按飞机和时间排好序再落盘，下游 pandas 读进来就是有序的
    points = points.orderBy("icao24", "event_time")

    (
        points.write
        .mode("overwrite")
        .parquet(POINTS_PATH)
    )

    out = spark.read.parquet(POINTS_PATH)
    kept = out.count()
    n_ac = out.select("icao24").distinct().count()

    print(f"[ml] 有效航迹点   : {kept}  （从 {raw_cnt} 条明细中筛出，"
          f"保留 {kept / raw_cnt * 100:.1f}%）")
    print(f"[ml] 参与建模飞机 : {n_ac} 架")
    if n_ac:
        print(f"[ml] 平均每架点数 : {kept / n_ac:.1f}")

    # ---- 数据是否够建模的体检 ----
    print("\n[ml] 数据量体检：")
    if n_ac < 50:
        print(f"      ⚠️  只有 {n_ac} 架飞机，样本太少，模型学不出稳定的正常模式。")
        print("          建议：把 opensky_producer.py 挂着连续跑 6-12 小时"
              "（欧洲白天时段 bbox 内流量最大）再回来。")
    elif kept / n_ac < 40:
        print(f"      ⚠️  平均每架只有 {kept / n_ac:.0f} 个点，航迹偏短，"
              "切出来的窗口数量有限。")
        print("          建议：延长单次连续采集时间，而不是多次短采集——"
              "序列的连续性比总量更重要。")
    else:
        print(f"      ✓ {n_ac} 架 × 平均 {kept / n_ac:.0f} 点，"
              "足够切出训练用的滑窗样本。")

    print("\n[ml] 样例（某架飞机的前 8 个点）:")
    if n_ac:
        first = out.select("icao24").limit(1).collect()[0][0]
        out.filter(f"icao24 = '{first}'") \
           .orderBy("event_time") \
           .select("icao24", "event_time", "altitude_m", "speed_kmh",
                   "heading_deg", "vertical_rate") \
           .show(8, truncate=False)

    print("=" * 62)
    print("  下一步： docker exec -it spark python3 "
          "/opt/spark/work-dir/jobs/ml_train.py")
    print("=" * 62)

    spark.stop()


if __name__ == "__main__":
    main()
