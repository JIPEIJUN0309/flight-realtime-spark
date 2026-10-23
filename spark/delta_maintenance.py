"""Delta 表维护：VACUUM 清物理文件，OPTIMIZE 合并小文件。

为什么需要（项目报告 M8）
--------------------------
- 航段表每批把当天的航段整段重写、最新状态表每批整表重写。Delta 只是把旧文件打上
  删除标记，物理文件一直留着：架构文档里航段表 121 MB，比约 80 万行的状态明细
  （96 MB）还大，而 PostgreSQL 里只有一万多个航段。
- bronze 和状态明细每 5 秒追加一批，每批一个小文件，文件数和元数据操作都会越跑越慢。

做法
----
- VACUUM：删掉超过保留期（默认 7 天 = 168 小时）的旧文件。保留期不要低于
  读表的最长回溯窗口，否则正在补数的作业会读不到文件。
- OPTIMIZE：只对追加型的大表（bronze、状态明细）做，把当天的小文件合并成
  大文件；重写型的小表（航段、最新状态、ADS）本来每批就整段重写，OPTIMIZE 收益不大。

跑法
----
    docker exec -it spark /opt/spark/bin/spark-submit \
        /opt/spark/work-dir/jobs/delta_maintenance.py

    # 只清物理文件 / 只合并小文件 / 换保留时长 / 只处理一张表
    ... --vacuum-only
    ... --optimize-only
    ... --retain-hours 72
    ... --table dwd_flight_leg

默认由 Airflow 的 flight_maintenance DAG 每天 02:30 UTC 跑一次。
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import SparkSession

DELTA_BASE = "/data/delta"

# 表名 → 路径。值和各作业里的默认路径保持一致，改路径要两边一起改。
TABLES = {
    "bronze_flights": f"{DELTA_BASE}/bronze_flights",
    "dwd_flight_state": f"{DELTA_BASE}/dwd_flight_state",
    "dwd_airport_event": f"{DELTA_BASE}/dwd_airport_event",
    "dwd_flight_leg": f"{DELTA_BASE}/dwd_flight_leg",
    "dws_airport_5min": f"{DELTA_BASE}/dws_airport_5min",
    "dws_airport_hourly": f"{DELTA_BASE}/dws_airport_hourly",
    "gold_country_5min": f"{DELTA_BASE}/gold_country_5min",
    "gold_overview_5min": f"{DELTA_BASE}/gold_overview_5min",
    "dq_result": f"{DELTA_BASE}/dq_result",
    "ads_latest_position": f"{DELTA_BASE}/ads/latest_position",
    "ads_flight_alert": f"{DELTA_BASE}/ads/flight_alert",
    "ads_airport_traffic": f"{DELTA_BASE}/ads/airport_traffic",
    "ads_airport_hourly": f"{DELTA_BASE}/ads/airport_hourly",
    "ads_country_share": f"{DELTA_BASE}/ads/country_share",
    "ads_realtime_kpi": f"{DELTA_BASE}/ads/realtime_kpi",
    "ads_ml_anomaly": f"{DELTA_BASE}/ads/ml_anomaly",
}

# 追加型表：每批新增一个小文件，值得合并
OPTIMIZE_TABLES = {"bronze_flights", "dwd_flight_state", "dwd_airport_event"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Delta-Maintenance")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        # VACUUM 的保留期检查：默认不允许低于 7 天。我们用默认值就不需要关，
        # 但保留这行注释，免得有人改成 --retain-hours 24 时被 Delta 拒绝还找不到原因。
        .getOrCreate()
    )


def table_exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable
    return DeltaTable.isDeltaTable(spark, path)


def vacuum(spark: SparkSession, name: str, path: str, retain_hours: int) -> None:
    started = time.time()
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retain_hours} HOURS")
    print(f"[maint] VACUUM {name}（保留 {retain_hours} 小时）用时 {time.time() - started:.1f} 秒")


def optimize(spark: SparkSession, name: str, path: str) -> None:
    started = time.time()
    spark.sql(f"OPTIMIZE delta.`{path}`")
    print(f"[maint] OPTIMIZE {name} 用时 {time.time() - started:.1f} 秒")


def main() -> None:
    p = argparse.ArgumentParser(description="Delta 表维护：VACUUM + OPTIMIZE")
    p.add_argument("--retain-hours", type=int, default=168,
                   help="VACUUM 保留时长（小时），默认 168 = 7 天")
    p.add_argument("--vacuum-only", action="store_true")
    p.add_argument("--optimize-only", action="store_true")
    p.add_argument("--table", action="append", default=None,
                   help="只处理指定表（可重复），默认全部")
    args = p.parse_args()

    names = args.table or list(TABLES)
    unknown = [n for n in names if n not in TABLES]
    if unknown:
        raise SystemExit(f"未知表名：{unknown}；可选：{sorted(TABLES)}")

    spark = build_spark()
    print("=" * 66)
    print(f"  Delta 维护：{len(names)} 张表，保留 {args.retain_hours} 小时")
    print("=" * 66)

    done = skipped = 0
    started = time.time()
    for name in names:
        path = TABLES[name]
        if not table_exists(spark, path):
            print(f"[maint] 跳过 {name}：表还不存在")
            skipped += 1
            continue
        if not args.optimize_only:
            vacuum(spark, name, path, args.retain_hours)
        if not args.vacuum_only and name in OPTIMIZE_TABLES:
            optimize(spark, name, path)
        done += 1

    print(f"[maint] 完成：处理 {done} 张、跳过 {skipped} 张，总用时 {time.time() - started:.1f} 秒")
    spark.stop()


if __name__ == "__main__":
    main()
