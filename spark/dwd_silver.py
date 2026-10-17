"""
DWD（Silver）层：批量清洗 → silver_flights（过渡期保留）
=========================================================
读 bronze 全量 → 按 flight_rules 的规则清洗 → 只留空中记录 → 覆盖写 silver_flights

★ 清洗规则不在这里写了
  过滤、去重、单位换算、派生字段、最近机场，全部来自 flight_rules.clean_states，
  和流式的 dwd_state.py 用的是同一份。以前这里一份、流作业里又一份，改了一边忘了
  另一边，流和批的结果就会悄悄对不上。

★ silver_flights 和 dwd_flight_state 的关系
  dwd_flight_state 是新的状态明细：含地面记录，流式写入。silver_flights 就是它的空中子集。
  dws_gold_batch、ads_export、异常检测还在读 silver，所以列名和含义保持不变。
  M4 把这些下游切到 dwd_flight_state（先过滤 on_ground = false）之后，这张表就可以下线。

为什么仍是批处理、全量覆盖：这是 Airflow 每 15 分钟跑一次的任务，M4 再改成按区间增量。
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flight_rules import AIRPORTS_CSV, airports_column, clean_states, load_airports

# ---------------- 路径配置 ----------------
BRONZE_PATH = "/data/delta/bronze_flights"
SILVER_PATH = "/data/delta/silver_flights"

# silver_flights 的列，和改造前完全一样，下游不用改
SILVER_COLUMNS = [
    "icao24", "callsign", "origin_country",
    "latitude", "longitude",
    "speed_kmh", "altitude_m", "altitude_ft",
    "heading_deg", "heading_dir",
    "vertical_rate", "flight_phase",
    "near_airport_iata", "near_airport_name", "near_airport_city",
    "airport_dist_km", "near_airport",
    "event_time", "ingest_time", "dt",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("DWD-Silver-Flights")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        # 开启 AQE（自适应查询执行）
        .config("spark.sql.adaptive.enabled", "true")
        # dt 按 UTC 日期分区
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  DWD Silver 层：共用清洗规则 → 空中记录")
    print(f"  读取 bronze : {BRONZE_PATH}")
    print(f"  机场维表     : {AIRPORTS_CSV}")
    print(f"  写入 silver : {SILVER_PATH}")
    print("=" * 60)

    bronze = spark.read.format("delta").load(BRONZE_PATH)
    bronze_cnt = bronze.count()
    airports = load_airports()
    print(f"[dwd] bronze 原始记录数: {bronze_cnt}")
    print(f"[dwd] 机场维表记录数: {len(airports)}")

    # ---- 1) 清洗：和流式作业同一份规则（含地面记录）----
    cleaned = clean_states(bronze, airports_column(airports))
    counts = cleaned.agg(
        F.count(F.lit(1)).alias("valid"),
        F.sum(F.col("on_ground").cast("int")).alias("ground"),
    ).first()
    valid_cnt, ground_cnt = counts["valid"], counts["ground"] or 0

    # ---- 2) 只留空中记录，列保持原样 ----
    silver = cleaned.where("NOT on_ground").select(*SILVER_COLUMNS)

    # ---- 3) 写入 silver 表（按天分区）----
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

    # ---- 4) 打印几条样例，直观看看加工效果 ----
    print("\n[dwd] Silver 表样例（前 10 条）:")
    spark.read.format("delta").load(SILVER_PATH).select(
        "callsign", "origin_country", "speed_kmh", "altitude_m",
        "heading_dir", "flight_phase", "near_airport_iata", "airport_dist_km"
    ).show(10, truncate=False)

    # ---- 5) 数据质量报告：过滤率是质量规则"过滤率稳定"的基线 ----
    def pct(n):
        return f"{n / bronze_cnt * 100:.1f}%" if bronze_cnt else "-"

    print("[dwd] 数据质量报告:")
    print(f"      bronze 原始            : {bronze_cnt:>8}")
    print(f"      无效或重复（过滤率）   : {bronze_cnt - valid_cnt:>8}  ({pct(bronze_cnt - valid_cnt)})")
    print(f"      清洗后（含地面）       : {valid_cnt:>8}")
    print(f"      其中地面记录           : {ground_cnt:>8}  ({pct(ground_cnt)})")
    print(f"      silver 最终（空中）    : {silver_cnt:>8}")

    spark.stop()


if __name__ == "__main__":
    main()
