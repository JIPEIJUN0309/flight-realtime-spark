"""
latest_position 的单元测试：每架飞机取最新一条、晚到的旧记录不覆盖、按数据时钟标记已离开、
重新出现时清除已离开、流式分批与全量重算一致、同一批重放不产生改动。
跑法：pytest tests/   （需要 pyspark 和 Java；不需要 Delta）
"""

from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

from latest_position import DEPARTED_AFTER_S, LATEST_COLUMNS, latest_updates

T0 = int(datetime(2026, 10, 20, 8, 0, tzinfo=timezone.utc).timestamp())

FIELDS = StructType([
    StructField("icao24", StringType()),
    StructField("t", LongType()),                 # 相对 T0 的秒数
    StructField("callsign", StringType()),
    StructField("on_ground", BooleanType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude_m", DoubleType()),
    StructField("vertical_rate", DoubleType()),
])


def utc(t):
    return datetime.fromtimestamp(T0 + t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def rec(icao24, t, callsign="SWR1", ground=False, lat=47.4, lon=8.5, alt=1000.0, vr=0.0):
    return (icao24, t, callsign, ground, lat, lon, alt, vr)


def states_df(spark, rows):
    """造状态明细：只给测试关心的几列，其余列按状态明细的类型补上。"""
    df = spark.createDataFrame(rows, FIELDS)
    return (
        df.withColumn("event_time", F.timestamp_seconds(F.col("t") + T0))
        .withColumn("kafka_ts", F.timestamp_seconds(F.col("t") + T0 + 2))
        .withColumn("origin_country", F.lit("Switzerland"))
        .withColumn("squawk", F.lit("1000"))
        .withColumn("speed_kmh", F.lit(400.0))
        .withColumn("flight_phase", F.when(F.col("on_ground"), "GROUND").otherwise("CRUISING"))
        .withColumn("heading_deg", F.lit(90.0))
        .withColumn("heading_dir", F.lit("E"))
        .withColumn("near_airport_iata", F.lit("ZRH"))
        .withColumn("near_airport_name", F.lit("Zurich Airport"))
        .withColumn("near_airport_city", F.lit("Zurich"))
        .withColumn("airport_dist_km", F.lit(12.0))
        .drop("t")
    )


def empty_table(spark):
    return states_df(spark, []).withColumn("departed", F.lit(False)).select(*LATEST_COLUMNS)


def as_dict(df):
    """{icao24: (最后出现 UTC, 呼号, 已离开)}；时间在 Spark 里转成字符串，不经过本机时区。"""
    rows = df.select("icao24", F.date_format("event_time", "yyyy-MM-dd HH:mm:ss").alias("ts"),
                     "callsign", "departed").collect()
    return {r["icao24"]: (r["ts"], r["callsign"], r["departed"]) for r in rows}


def frozen(spark, df):
    # 收集后重建：每批的表都是浅计划，不带之前各批的血缘
    return spark.createDataFrame(df.collect(), df.schema)


def apply(spark, table, updates):
    """像 MERGE 一样按 icao24 写回：更新的行替换旧行，新飞机插入。"""
    kept = table.join(updates.select("icao24"), "icao24", "left_anti")
    return frozen(spark, kept.unionByName(updates))


def test_newest_record_wins(spark):
    rows = [rec("a", 0, callsign="OLD"), rec("a", 10, callsign="NEW"), rec("b", 5)]
    out = as_dict(latest_updates(empty_table(spark), states_df(spark, rows), utc(10)))
    assert out == {"a": (utc(10), "NEW", False), "b": (utc(5), "SWR1", False)}


def test_late_older_record_does_not_overwrite(spark):
    table = frozen(spark, latest_updates(empty_table(spark), states_df(spark, [rec("a", 20, callsign="NEW")]),
                                         utc(20)))
    # 晚到一条更早的记录（或者同一批重放）：不改位置，也不产生要写的行
    late = latest_updates(table, states_df(spark, [rec("a", 10, callsign="OLD")]), utc(20))
    assert late.count() == 0
    replay = latest_updates(table, states_df(spark, [rec("a", 20, callsign="NEW")]), utc(20))
    assert replay.count() == 0


def test_departed_after_one_minute_of_data_time(spark):
    table = frozen(spark, latest_updates(empty_table(spark), states_df(spark, [rec("a", 0), rec("b", 0)]),
                                         utc(0)))
    # 数据时钟走到 60 秒：正好 1 分钟不算离开；61 秒才算。b 一直有新记录
    at_60 = latest_updates(table, states_df(spark, [rec("b", 60)]), utc(DEPARTED_AFTER_S))
    assert as_dict(at_60) == {"b": (utc(60), "SWR1", False)}
    table = apply(spark, table, at_60)
    at_61 = latest_updates(table, states_df(spark, [rec("b", 61)]), utc(DEPARTED_AFTER_S + 1))
    assert as_dict(at_61) == {"a": (utc(0), "SWR1", True), "b": (utc(61), "SWR1", False)}


def test_reappearing_aircraft_clears_departed(spark):
    table = frozen(spark, latest_updates(empty_table(spark), states_df(spark, [rec("a", 0)]), utc(0)))
    table = apply(spark, table, latest_updates(table, states_df(spark, [rec("b", 120)]), utc(120)))
    assert as_dict(table)["a"][2] is True
    back = latest_updates(table, states_df(spark, [rec("a", 130, callsign="BACK")]), utc(130))
    assert as_dict(back) == {"a": (utc(130), "BACK", False)}


def test_machine_time_is_not_used(spark):
    # 时钟停在最后一张快照：多久以后再算，结果都一样（采集器停了，地图不会被清空）
    table = latest_updates(empty_table(spark), states_df(spark, [rec("a", 0)]), utc(0))
    assert as_dict(table) == {"a": (utc(0), "SWR1", False)}


def scenario():
    """10 分钟、5 架飞机，10 秒一张快照：有中途消失又回来的、只出现一次的、一直在的。"""
    rows = []
    rows += [rec("a", t, alt=3000.0 - t) for t in range(0, 600, 10)]                 # 一直在
    rows += [rec("b", t) for t in range(0, 120, 10)]                                  # 2 分钟后消失
    rows += [rec("b", t, callsign="B2") for t in range(300, 400, 10)]                 # 3 分钟后回来，又消失
    rows += [rec("c", 200)]                                                           # 只出现一次
    rows += [rec("d", t, ground=True, alt=None) for t in range(0, 600, 30)]           # 地面，30 秒一条
    rows += [rec("e", t) for t in range(540, 600, 10)]                                # 最后一分钟才出现
    return rows


def test_stream_equals_full_recompute(spark):
    rows = scenario()
    expected = as_dict(latest_updates(empty_table(spark), states_df(spark, rows), utc(590)))

    for batch_seconds in (10, 40):
        table = empty_table(spark)
        clock = None
        for lo in range(0, 600, batch_seconds):
            batch_rows = [r for r in rows if lo <= r[1] < lo + batch_seconds]
            if not batch_rows:
                continue
            # 数据时钟只往后走：取已处理数据里最晚的快照时间
            clock = max(clock or utc(0), utc(max(r[1] for r in batch_rows)))
            table = apply(spark, table, latest_updates(table, states_df(spark, batch_rows), clock))
        assert as_dict(table) == expected, f"{batch_seconds} 秒一批"
    # 结尾时 b、c 早已离开；a、d、e 还在（d 最后一条在 570 秒，离 590 秒不到 1 分钟）
    assert {k for k, v in expected.items() if v[2]} == {"b", "c"}


def test_replayed_batch_changes_nothing(spark):
    rows = scenario()
    first = [r for r in rows if r[1] < 300]
    table = frozen(spark, latest_updates(empty_table(spark), states_df(spark, first), utc(290)))
    assert latest_updates(table, states_df(spark, [r for r in first if r[1] >= 280]), utc(290)).count() == 0
