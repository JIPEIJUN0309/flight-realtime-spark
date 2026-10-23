"""一次性清理：删掉演练时写进正式湖的测试飞机（icao24=000000）。

项目报告 L10：应急代码演练把测试飞机写进了正式的 flights 主题和湖，应当删除。
这个脚本不是流水线的一部分，只是留下可复现的清理动作。
"""
from __future__ import annotations

from delta import DeltaTable
from pyspark.sql import SparkSession

TABLES = [
    "/data/delta/bronze_flights",
    "/data/delta/dwd_flight_state",
    "/data/delta/dwd_airport_event",
    "/data/delta/dwd_flight_leg",
    "/data/delta/dws_airport_5min",
    "/data/delta/ads/latest_position",
    "/data/delta/ads/flight_alert",
    "/data/delta/ads/ml_anomaly",
]

TEST_ICAO = "000000"


def main() -> None:
    spark = (
        SparkSession.builder.appName("Clean-Test-Aircraft")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    for path in TABLES:
        if not DeltaTable.isDeltaTable(spark, path):
            print(f"[clean] 跳过 {path}：表不存在")
            continue
        try:
            n = spark.read.format("delta").load(path).where(f"icao24 = '{TEST_ICAO}'").count()
        except Exception as e:  # noqa: BLE001 —— 没有 icao24 列的表直接跳过
            print(f"[clean] 跳过 {path}：{type(e).__name__}")
            continue
        if n == 0:
            print(f"[clean] {path}: 没有 {TEST_ICAO} 的行")
            continue
        DeltaTable.forPath(spark, path).delete(f"icao24 = '{TEST_ICAO}'")
        left = spark.read.format("delta").load(path).where(f"icao24 = '{TEST_ICAO}'").count()
        print(f"[clean] {path}: 删掉 {n} 行，剩 {left} 行")
    spark.stop()


if __name__ == "__main__":
    main()
