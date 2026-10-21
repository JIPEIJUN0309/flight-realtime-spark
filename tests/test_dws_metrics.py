"""
dws_metrics 的单元测试：进近/离场口径、架数与架次、5 分钟和小时窗口、时间对齐。
跑法：pytest tests/   （需要 pyspark 和 Java；不需要 Delta）
"""

from datetime import datetime, timezone

import pytest
from pyspark.sql import types as T

import dws_metrics as m

# 2026-10-20 12:00:00 UTC，5 分钟和整点的边界都从这里开始算
T0 = int(datetime(2026, 10, 20, 12, 0, tzinfo=timezone.utc).timestamp())

STATE_SCHEMA = T.StructType([
    T.StructField("icao24", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("origin_country", T.StringType()),
    T.StructField("altitude_m", T.DoubleType()),
    T.StructField("speed_kmh", T.DoubleType()),
    T.StructField("vertical_rate", T.DoubleType()),
    T.StructField("flight_phase", T.StringType()),
    T.StructField("near_airport_iata", T.StringType()),
    T.StructField("near_airport_city", T.StringType()),
    T.StructField("airport_dist_km", T.DoubleType()),
])

EVENT_SCHEMA = T.StructType([
    T.StructField("icao24", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("airport_iata", T.StringType()),
    T.StructField("airport_city", T.StringType()),
])


def ts(seconds: int) -> datetime:
    """相对 T0 的秒数 → UTC 时间戳。"""
    return datetime.fromtimestamp(T0 + seconds, tz=timezone.utc).replace(tzinfo=None)


def state(icao24="4b1814", t=0, dist=10.0, alt=2000.0, vr=-3.0,
          iata="ZRH", city="Zurich", country="Switzerland"):
    """一条空中状态。默认是 ZRH 10 km、2,000 m、正在下降 —— 标准进近。"""
    phase = "DESCENDING" if vr < -1 else ("CLIMBING" if vr > 1 else "CRUISING")
    return (icao24, ts(t), country, alt, 250.0, vr, phase, iata, city, dist)


def event(icao24="4b1814", t=0, kind="TAKEOFF", iata="ZRH", city="Zurich"):
    return (icao24, ts(t), kind, iata, city)


def states(spark, rows):
    return spark.createDataFrame(rows, STATE_SCHEMA)


def events(spark, rows):
    return spark.createDataFrame(rows, EVENT_SCHEMA)


# ================================================================
#  进近 / 离场口径
# ================================================================
def test_approach_counts_only_low_descending_near_airport(spark):
    """三类样本：高空下降路过、低空平飞、低空下降。只有低空下降算进近。"""
    rows = [
        # 高空下降：10 km 但 8,000 m，是路过，不是进近
        state("aaaaaa", alt=8000.0, vr=-6.0),
        # 低空平飞：高度和距离都对，但不在下降
        state("bbbbbb", alt=2000.0, vr=0.0),
        # 低空下降：只有这个算
        state("cccccc", alt=2000.0, vr=-3.0),
    ]
    row = (m.airport_5min(states(spark, rows))
           .agg({"arriving_cnt": "sum", "departing_cnt": "sum"}).first())
    assert row["sum(arriving_cnt)"] == 1
    assert row["sum(departing_cnt)"] == 0

    # 架数和进近架数是两个口径：这一个 ZRH 窗口里 3 架飞机都算在飞（aircraft_cnt=3），
    # 但只有 1 架满足进近条件（低空、在下降）。上一段已经断言 arriving_cnt 合计为 1。
    counted = (m.airport_5min(states(spark, rows))
               .where("arriving_cnt > 0").select("aircraft_cnt").first())
    assert counted["aircraft_cnt"] == 3


def test_approach_distance_and_altitude_boundaries(spark):
    """30 km 和 3,000 m 是闭区间；3,001 m 或 30.1 km 就不算。"""
    rows = [
        state("edge30", dist=30.0, alt=3000.0, vr=-2.0),
        state("edge31", dist=30.1, alt=3000.0, vr=-2.0),
        state("edgealt", dist=29.9, alt=3001.0, vr=-2.0),
    ]
    row = (m.airport_5min(states(spark, rows))
           .agg({"arriving_cnt": "sum"}).first())
    assert row["sum(arriving_cnt)"] == 1


def test_vertical_rate_boundary(spark):
    """垂直速率 ≤ −1 才算进近，≥ 1 才算离场；−0.9 和 0.9 都不算。"""
    rows = [
        state("arr1", vr=-1.0),
        state("arr09", vr=-0.9),
        state("dep1", vr=1.0),
        state("dep09", vr=0.9),
    ]
    row = (m.airport_5min(states(spark, rows))
           .agg({"arriving_cnt": "sum", "departing_cnt": "sum"}).first())
    assert row["sum(arriving_cnt)"] == 1
    assert row["sum(departing_cnt)"] == 1


# ================================================================
#  架数：同一架飞机在窗口里出现多次只算一架
# ================================================================
def test_aircraft_cnt_is_distinct_aircraft_not_records(spark):
    rows = [
        state("same", t=0), state("same", t=10), state("same", t=20),
        state("other", t=30),
    ]
    row = (m.airport_5min(states(spark, rows)).agg(
        {"aircraft_cnt": "sum", "record_cnt": "sum"}).first())
    assert row["sum(aircraft_cnt)"] == 2      # 两架飞机
    assert row["sum(record_cnt)"] == 4        # 四条记录


def test_window_boundary_splits_into_two_rows(spark):
    """12:04:59 和 12:05:00 属于两个相邻窗口，同一个机场两行。"""
    rows = [state("plane", t=299), state("plane", t=300)]
    out = m.airport_5min(states(spark, rows)).orderBy("window_start").collect()
    assert len(out) == 2
    assert {r["aircraft_cnt"] for r in out} == {1}


# ================================================================
#  小时起降：数事件，不去重
# ================================================================
def test_airport_hourly_counts_movements(spark):
    """同一架飞机一小时里起两次、落一次，是 3 架次、1 架飞机。"""
    rows = [
        event(t=5, kind="TAKEOFF"),
        event(t=3605, kind="LANDING"),
        event(t=3700, kind="TAKEOFF"),
    ]
    out = m.airport_hourly(events(spark, rows)).orderBy("hour_start").collect()
    assert len(out) == 2

    first = out[0]
    assert first["takeoff_cnt"] == 1 and first["landing_cnt"] == 0
    assert first["movement_cnt"] == 1

    second = out[1]
    assert second["takeoff_cnt"] == 1 and second["landing_cnt"] == 1
    assert second["movement_cnt"] == 2


def test_airport_hourly_keeps_airports_apart(spark):
    rows = [
        event(t=0, iata="ZRH"), event(t=60, iata="GVA", city="Geneva"),
    ]
    out = m.airport_hourly(events(spark, rows)).collect()
    assert {r["airport_iata"] for r in out} == {"ZRH", "GVA"}
    assert sum(r["takeoff_cnt"] for r in out) == 2


# ================================================================
#  国家占比与总览
# ================================================================
def test_country_share_sums_to_100(spark):
    rows = [state("a", country="Switzerland"), state("b", country="Germany"),
            state("c", country="Germany")]
    out = m.country_5min(states(spark, rows)).collect()
    assert {r["origin_country"] for r in out} == {"Switzerland", "Germany"}
    assert sum(r["pct_of_window"] for r in out) == pytest.approx(100.0, abs=0.01)
    germany = [r for r in out if r["origin_country"] == "Germany"][0]
    assert germany["aircraft_cnt"] == 2
    assert germany["rank_in_window"] == 1


def test_overview_counts_aircraft_and_phases(spark):
    rows = [
        state("a", vr=-3.0),        # 下降
        state("b", vr=3.0),         # 爬升
        state("c", vr=0.0),         # 平飞
        state("a", t=60),           # 同一架飞机，不重复计入
    ]
    row = (m.overview_5min(states(spark, rows)).agg(
        {"aircraft_cnt": "sum", "descending_cnt": "sum",
         "climbing_cnt": "sum", "cruising_cnt": "sum"}).first())
    assert row["sum(aircraft_cnt)"] == 3
    assert row["sum(descending_cnt)"] == 1
    assert row["sum(climbing_cnt)"] == 1
    assert row["sum(cruising_cnt)"] == 1


# ================================================================
#  时间对齐：区间必须落在窗口边界上，重算才幂等
# ================================================================
def test_floor_minutes():
    assert m.floor_minutes("2026-10-20 12:07:33", 5) == "2026-10-20 12:05:00"
    assert m.floor_minutes("2026-10-20 12:05:00", 5) == "2026-10-20 12:05:00"
    assert m.floor_minutes("2026-10-20 12:04:59", 5) == "2026-10-20 12:00:00"
    assert m.floor_minutes("2026-10-20 23:59:59", 5) == "2026-10-20 23:55:00"


def test_floor_hours():
    assert m.floor_hours("2026-10-20 12:59:59") == "2026-10-20 12:00:00"
    assert m.floor_hours("2026-10-20 00:00:00") == "2026-10-20 00:00:00"
