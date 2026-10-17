"""
flight_rules 的单元测试：有效性、去重、派生字段、最近机场、流批一致。
跑法：pytest tests/   （需要 pyspark 和 Java；不需要 Delta）
"""

import math
from datetime import date, datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

from conftest import AIRPORTS_CSV
from flight_rules import (
    KEY_COLUMNS, airports_column, clean_states, haversine_km, load_airports, valid_states,
)

# ODS（bronze_flights）里的业务字段和 Kafka 元数据，与 ods_bronze.py 一致
ODS_FIELDS = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", IntegerType()),
    StructField("category", IntegerType()),
    StructField("snapshot_time", LongType()),
    StructField("topic", StringType()),
    StructField("partition", IntegerType()),
    StructField("offset", LongType()),
])

T0 = int(datetime(2026, 10, 17, 10, 0, tzinfo=timezone.utc).timestamp())

ZRH = (47.4647, 8.54917)


def rec(icao24="4b1814", t=0, offset=0, **overrides):
    """一条 ODS 记录：默认是 ZRH 附近、正在下降的一架飞机。t 是相对 T0 的秒数。"""
    r = dict(
        icao24=icao24, callsign="SWR123 ", origin_country="Switzerland",
        time_position=T0 + t - 2, last_contact=T0 + t - 1,
        longitude=8.60, latitude=47.40, baro_altitude=1200.0, on_ground=False,
        velocity=80.0, true_track=140.0, vertical_rate=-4.0, geo_altitude=1300.0,
        squawk="1000", spi=False, position_source=0, category=None,
        snapshot_time=T0 + t, topic="flights", partition=0, offset=offset,
    )
    r.update(overrides)
    return r


def ods_df(spark, records):
    """和 ods_bronze.py 一样补上 event_time、kafka_ts、ingest_time。

    kafka_ts 随 offset 变化，方便判断去重时留下的是哪一条。
    """
    df = spark.createDataFrame([tuple(r[f.name] for f in ODS_FIELDS) for r in records], ODS_FIELDS)
    return (
        df.withColumn("kafka_ts", F.timestamp_seconds(F.col("snapshot_time") + 1 + F.col("offset")))
        .withColumn("event_time", F.timestamp_seconds("snapshot_time"))
        .withColumn("ingest_time", F.timestamp_seconds(F.col("snapshot_time") + 3 + F.col("offset")))
    )


@pytest.fixture(scope="module")
def airports():
    return airports_column(load_airports(AIRPORTS_CSV))


def clean_rows(spark, airports, records):
    return {r["icao24"]: r for r in clean_states(ods_df(spark, records), airports).collect()}


def py_haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = (math.sin(math.radians(lat2 - lat1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2)
    return 6371 * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------- 有效性
INVALID = {
    "no_icao24": dict(icao24=None),
    "no_snapshot_time": dict(snapshot_time=None),
    "no_on_ground": dict(on_ground=None),
    "no_latitude": dict(latitude=None),
    "latitude_out_of_range": dict(latitude=91.0),
    "longitude_out_of_range": dict(longitude=-180.5),
    "too_high": dict(baro_altitude=15000.1),
    "too_low": dict(baro_altitude=-500.1),
    "too_fast": dict(velocity=400.1),
    "negative_speed": dict(velocity=-0.1),
}

VALID_EDGES = {
    "altitude_null": dict(baro_altitude=None),
    "altitude_min": dict(baro_altitude=-500.0),
    "altitude_max": dict(baro_altitude=15000.0),
    "speed_null": dict(velocity=None),
    "speed_zero": dict(velocity=0.0),
    "speed_max": dict(velocity=400.0),
    "on_ground": dict(on_ground=True, baro_altitude=None, velocity=3.0, vertical_rate=None),
}


def test_invalid_records_are_dropped(spark, airports):
    # 每条无效记录用自己的 icao24，最后加一条有效的作对照
    records = [rec(**{"icao24": name, **o}) for name, o in INVALID.items()]
    records.append(rec(icao24="control"))
    survivors = [r["icao24"] for r in clean_states(ods_df(spark, records), airports).collect()]
    assert survivors == ["control"]


def test_boundary_values_are_kept(spark, airports):
    records = [rec(icao24=name, **o) for name, o in VALID_EDGES.items()]
    kept = clean_rows(spark, airports, records)
    assert set(kept) == set(VALID_EDGES)


# ---------------------------------------------------------------- 派生字段
@pytest.mark.parametrize("on_ground, vertical_rate, expected", [
    (True, None, "GROUND"),
    (True, 5.0, "GROUND"),          # 地面记录不看垂直速率
    (False, 2.0, "CLIMBING"),
    (False, -2.0, "DESCENDING"),
    (False, 1.0, "CRUISING"),       # 阈值是严格大于 1 m/s
    (False, 0.5, "CRUISING"),
    (False, None, "UNKNOWN"),
])
def test_flight_phase(spark, airports, on_ground, vertical_rate, expected):
    row = clean_states(ods_df(spark, [rec(on_ground=on_ground, vertical_rate=vertical_rate)]),
                       airports).first()
    assert row["flight_phase"] == expected


@pytest.mark.parametrize("track, expected", [
    (0.0, "N"), (22.4, "N"), (22.5, "NE"), (90.0, "E"), (180.0, "S"),
    (337.4, "NW"), (337.5, "N"), (359.9, "N"), (None, "UNKNOWN"),
])
def test_heading_dir(spark, airports, track, expected):
    row = clean_states(ods_df(spark, [rec(true_track=track)]), airports).first()
    assert row["heading_dir"] == expected


def test_units_callsign_and_times(spark, airports):
    rows = clean_rows(spark, airports, [
        rec(icao24="a1", callsign=" SWR123 "),
        rec(icao24="a2", callsign="   "),
        rec(icao24="a3", callsign=None),
    ])
    a1 = rows["a1"]
    assert a1["callsign"] == "SWR123"
    assert rows["a2"]["callsign"] == "UNKNOWN"
    assert rows["a3"]["callsign"] == "UNKNOWN"
    assert a1["speed_kmh"] == 288.0                  # 80 m/s
    assert a1["altitude_m"] == 1200.0
    assert a1["altitude_ft"] == 3937.0               # 1200 × 3.28084 = 3937.0
    assert a1["squawk"] == "1000" and a1["on_ground"] is False
    # collect() 出来的时间戳会按本机时区转成 naive datetime，用 .timestamp() 换回秒数再比
    assert a1["event_time"].timestamp() == T0
    assert a1["position_time"].timestamp() == T0 - 2
    assert a1["last_contact_time"].timestamp() == T0 - 1
    assert a1["dt"] == date(2026, 10, 17)


def test_dt_is_utc_date(spark, airports):
    # UTC 23:30 在苏黎世已经是第二天，dt 仍取 UTC 日期
    late = rec(t=13 * 3600 + 30 * 60)
    assert clean_states(ods_df(spark, [late]), airports).first()["dt"] == date(2026, 10, 17)


def test_requires_utc_session(spark, airports):
    df = ods_df(spark, [rec()])
    spark.conf.set("spark.sql.session.timeZone", "Europe/Zurich")
    try:
        with pytest.raises(ValueError, match="UTC"):
            clean_states(df, airports)
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


# ---------------------------------------------------------------- 去重
def test_dedupe_keeps_smallest_offset(spark, airports):
    rows = clean_states(ods_df(spark, [
        rec(offset=9, callsign="LATE"),
        rec(offset=4, callsign="FIRST"),
        rec(offset=7, callsign="MIDDLE"),
    ]), airports).collect()
    assert len(rows) == 1
    assert rows[0]["callsign"] == "FIRST"
    assert rows[0]["kafka_ts"].timestamp() == T0 + 1 + 4


def test_filter_runs_before_dedupe(spark, airports):
    # offset 小的那条无效：先过滤再去重，留下的是有效的那条；反过来做会两条都丢
    rows = clean_states(ods_df(spark, [
        rec(offset=4, baro_altitude=20000.0, callsign="BAD"),
        rec(offset=9, callsign="GOOD"),
    ]), airports).collect()
    assert [r["callsign"] for r in rows] == ["GOOD"]


def test_different_snapshots_are_not_duplicates(spark, airports):
    rows = clean_states(ods_df(spark, [rec(t=0, offset=1), rec(t=10, offset=2)]), airports).collect()
    assert len(rows) == 2


# ---------------------------------------------------------------- 最近机场
def test_nearest_airport_matches_python(spark, airports):
    lat, lon = 47.40, 8.60
    expected = min(load_airports(AIRPORTS_CSV),
                   key=lambda a: py_haversine_km(lat, lon, a["lat"], a["lon"]))
    row = clean_states(ods_df(spark, [rec(latitude=lat, longitude=lon)]), airports).first()
    assert row["near_airport_iata"] == expected["iata"] == "ZRH"
    assert row["near_airport_city"] == "Zurich"
    assert row["airport_dist_km"] == round(py_haversine_km(lat, lon, *ZRH), 1)
    assert row["near_airport"] is True


def test_nearest_airport_tie_breaks_by_iata(spark):
    # 两个机场离 (0, 0) 一样远，取 IATA 代码靠前的，结果每次都一样
    tie = airports_column([
        dict(iata="BBB", name="B", city="B", lat=0.0, lon=1.0),
        dict(iata="AAA", name="A", city="A", lat=0.0, lon=-1.0),
    ])
    row = clean_states(ods_df(spark, [rec(latitude=0.0, longitude=0.0)]), tie).first()
    assert row["near_airport_iata"] == "AAA"


@pytest.mark.parametrize("dist_km, near", [(49.9, True), (50.04, True), (50.06, False)])
def test_near_airport_uses_rounded_distance(spark, dist_km, near):
    # 机场在 (0, 0)，飞机沿经线往北 dist_km：50.04 显示为 50.0，判为附近；50.06 显示为 50.1
    one = airports_column([dict(iata="XXX", name="X", city="X", lat=0.0, lon=0.0)])
    lat = math.degrees(dist_km / 6371)
    row = clean_states(ods_df(spark, [rec(latitude=lat, longitude=0.0)]), one).first()
    assert row["near_airport"] is near


def test_haversine_sql_matches_python(spark):
    sql = haversine_km("47.4647", "8.54917", "50.03333", "8.57056")   # ZRH → FRA
    value = spark.range(1).select(F.expr(sql).alias("d")).first()["d"]
    assert value == pytest.approx(py_haversine_km(47.4647, 8.54917, 50.03333, 8.57056), rel=1e-12)
    assert value == pytest.approx(285.5, abs=1)


# ---------------------------------------------------------------- 维表
def test_load_airports_real_file():
    airports = load_airports(AIRPORTS_CSV)
    codes = {a["iata"] for a in airports}
    assert len(airports) == 22
    assert {"ZRH", "BGY", "LUG"} <= codes


def test_load_airports_rejects_duplicates(tmp_path):
    path = tmp_path / "airports.csv"
    path.write_text(
        "iata,icao,airport_name,city,country,latitude,longitude\n"
        "ZRH,LSZH,Zurich Airport,Zurich,Switzerland,47.4647,8.54917\n"
        "ZRH,LSZH,Zurich Airport,Zurich,Switzerland,47.4647,8.54917\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="重复"):
        load_airports(str(path))


# ---------------------------------------------------------------- 流式可用
def test_valid_states_works_on_a_stream(spark, tmp_path):
    stream = spark.readStream.schema(ODS_FIELDS).parquet(str(tmp_path)) \
        .withColumn("event_time", F.timestamp_seconds("snapshot_time"))
    filtered = valid_states(stream)
    assert filtered.isStreaming
    assert "icao24" in filtered.columns


# ---------------------------------------------------------------- 流批一致
def test_microbatch_insert_if_absent_equals_batch(spark, airports):
    """模拟流式：按 Kafka 到达顺序切成几个微批，每批清洗后只插入目标里没有的主键
    （等价于 MERGE ... WHEN NOT MATCHED THEN INSERT），结果应与一次性批量清洗完全一致。"""
    records, offset = [], 0
    for t in range(0, 60, 10):                         # 6 张快照
        for icao24 in ("a1", "a2", "a3"):
            records.append(rec(icao24=icao24, t=t, offset=offset,
                               on_ground=(icao24 == "a3" and t >= 30)))
            offset += 1
    # 跨微批的重复：同一快照被重发，offset 更大
    records.append(rec(icao24="a1", t=0, offset=offset, callsign="RESENT")); offset += 1
    # 先到的一条无效、后到的同主键记录有效
    records.append(rec(icao24="a4", t=10, offset=offset, baro_altitude=99999.0)); offset += 1
    records.append(rec(icao24="a4", t=10, offset=offset)); offset += 1
    # 同一微批内的重复
    records.append(rec(icao24="a2", t=50, offset=offset, callsign="DUP")); offset += 1

    by_offset = sorted(records, key=lambda r: r["offset"])
    microbatches = [by_offset[0:7], by_offset[7:15], by_offset[15:19], by_offset[19:]]

    target = None
    for batch in microbatches:
        cleaned = clean_states(ods_df(spark, batch), airports)
        new_rows = cleaned if target is None else cleaned.join(target, list(KEY_COLUMNS), "left_anti")
        target = new_rows if target is None else target.unionByName(new_rows)

    expected = clean_states(ods_df(spark, records), airports)
    assert target.exceptAll(expected).count() == 0
    assert expected.exceptAll(target).count() == 0
    assert target.groupBy(*KEY_COLUMNS).count().where("count > 1").count() == 0
    assert target.where("callsign IN ('RESENT', 'DUP')").count() == 0
    assert target.where("icao24 = 'a4'").count() == 1
