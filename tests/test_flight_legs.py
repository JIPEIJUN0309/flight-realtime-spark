"""
flight_legs 的单元测试：起降判定（含滑行抖动修正）、航段切分（需求文档"验收方式"里的边界用例）、
丢失原因、流式分批结果与批量重算一致、微批重放结果不变。
跑法：pytest tests/   （需要 pyspark 和 Java；不需要 Delta）
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from flight_legs import (
    EVENT_KEY, LEG_COLUMNS, LEG_GAP_S, batch_bounds, compute_all, stream_step,
)

T0 = int(datetime(2026, 10, 17, 10, 0, tzinfo=timezone.utc).timestamp())
# 7 月的历史数据按旧的采集范围（北纬 45–50°、东经 5–10°）采集；相对 T0 的秒数
JULY = int(datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc).timestamp()) - T0
MIN = 60

STATE_FIELDS = StructType([
    StructField("icao24", StringType()),
    StructField("t", LongType()),                 # 相对 T0 的秒数
    StructField("on_ground", BooleanType()),
    StructField("callsign", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude_m", DoubleType()),
    StructField("speed_kmh", DoubleType()),
    StructField("near_airport_iata", StringType()),
    StructField("near_airport_name", StringType()),
    StructField("near_airport_city", StringType()),
    StructField("airport_dist_km", DoubleType()),
    StructField("flight_phase", StringType()),
])

AIRPORTS = {
    "ZRH": ("Zurich Airport", "Zurich"),
    "FRA": ("Frankfurt Airport", "Frankfurt"),
    "CGN": ("Cologne Bonn Airport", "Cologne"),
    "LYS": ("Lyon Saint-Exupery Airport", "Lyon"),
}


def st(icao24, t, ground, iata="ZRH", dist=1.0, alt=None, callsign="SWR1", lat=47.46, lon=8.55,
       speed=250.0, phase=None):
    """一条状态明细。默认在 ZRH 1 km 处、时速 250 km/h；空中默认 1,000 m 平飞，地面默认 0 m。"""
    name, city = AIRPORTS[iata]
    altitude = alt if alt is not None else (0.0 if ground else 1000.0)
    phase = phase or ("GROUND" if ground else "CRUISING")
    return (icao24, t, ground, callsign, lat, lon, altitude, speed, iata, name, city, dist, phase)


def air(icao24, t, **kw):
    return st(icao24, t, False, **kw)


def gnd(icao24, t, **kw):
    return st(icao24, t, True, **kw)


def states_df(spark, rows):
    df = spark.createDataFrame(rows, STATE_FIELDS)
    return (
        df.withColumn("event_time", F.timestamp_seconds(F.col("t") + T0))
        .withColumn("kafka_ts", F.timestamp_seconds(F.col("t") + T0 + 1))
        .withColumn("dt", F.to_date("event_time"))
        .drop("t")
    )


def to_utc_naive(col):
    # 测试里比较时间：collect 回来的 datetime 按本机时区换算，先在 Spark 里转成 UTC 字符串
    return F.date_format(col, "yyyy-MM-dd HH:mm:ss")


def legs_as_text(legs_df):
    """把航段转成可比较的元组：时间列在 Spark 里格式化成 UTC 字符串，不经过 Python datetime。"""
    cols = []
    for c in LEG_COLUMNS:
        if c in ("start_time", "last_seen", "end_time"):
            cols.append(to_utc_naive(F.col(c)).alias(c))
        elif c == "dt":
            cols.append(F.col(c).cast("string").alias(c))
        else:
            cols.append(F.col(c))
    return sorted(tuple(r) for r in legs_df.select(*cols).collect())


def events_as_text(events_df):
    cols = [to_utc_naive(F.col(c)).alias(c) if c in ("event_time", "prev_event_time", "kafka_ts")
            else F.col(c).cast("string").alias(c) if c == "dt" else F.col(c)
            for c in events_df.columns]
    return sorted(tuple(r) for r in events_df.select(*cols).collect())


def utc(t):
    return datetime.fromtimestamp(T0 + t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def leg_text_rows(spark, rows, clock_t=None):
    clock = utc(clock_t) if clock_t is not None else None
    _, legs = compute_all(states_df(spark, rows), clock)
    keyed = {}
    for r in legs.select(
        "leg_id", "icao24", "status", "lost_reason", "departure_kind", "departure_iata", "arrival_iata",
        "max_altitude_m", "last_altitude_m", "last_flight_phase", "last_airport_iata",
        "last_airport_dist_km", "callsign",
        to_utc_naive(F.col("start_time")).alias("start"),
        to_utc_naive(F.col("last_seen")).alias("last"),
        to_utc_naive(F.col("end_time")).alias("end"),
    ).collect():
        keyed.setdefault(r["icao24"], []).append(r)
    for v in keyed.values():
        v.sort(key=lambda r: r["start"])
    return keyed


def lost_reasons(spark, rows, clock_t):
    return {k: [l["lost_reason"] for l in v] for k, v in leg_text_rows(spark, rows, clock_t).items()}


def event_list(spark, rows):
    events, _ = compute_all(states_df(spark, rows))
    return sorted(
        (r["icao24"], r["event_type"], r["airport_iata"], r["t"])
        for r in events.select(
            "icao24", "event_type", "airport_iata",
            (F.unix_timestamp("event_time") - F.lit(T0)).alias("t"),
        ).collect()
    )


# ================================================================
#  起降判定
# ================================================================
def test_landing_and_takeoff(spark):
    rows = [air("a", 0), gnd("a", 10), gnd("a", 20),        # 落地，之后一直在地面不再计
            gnd("b", 0), air("b", 10, iata="FRA")]           # 在 FRA 起飞
    assert event_list(spark, rows) == [("a", "LANDING", "ZRH", 10), ("b", "TAKEOFF", "FRA", 10)]


def test_flip_gap_boundary(spark):
    rows = [air("a", 0), gnd("a", 60),                       # 正好 60 秒：算
            air("b", 0), gnd("b", 61)]                       # 61 秒：前后之间没看到，不算
    assert event_list(spark, rows) == [("a", "LANDING", "ZRH", 60)]


def test_distance_boundary(spark):
    rows = [air("a", 0), gnd("a", 10, dist=5.0),             # 5.0 km：算
            air("b", 0), gnd("b", 10, dist=5.1),             # 5.1 km：不算
            air("c", 0), gnd("c", 10, dist=None)]            # 距离为空：不算
    assert event_list(spark, rows) == [("a", "LANDING", "ZRH", 10)]


def test_first_record_never_an_event(spark):
    assert event_list(spark, [gnd("a", 0), gnd("a", 10)]) == []


# ================================================================
#  滑行抖动：机场 5 km 内、时速低于 100 km/h 的"空中"记录算在地面
# ================================================================
def test_taxi_flicker_is_not_an_event(spark):
    # 落地后滑行时标记跳成空中（30 km/h）又跳回地面：只有一次落地，不多出起飞和航段
    rows = [air("a", 0), gnd("a", 10), air("a", 20, speed=30.0), gnd("a", 30), gnd("a", 40)]
    assert event_list(spark, rows) == [("a", "LANDING", "ZRH", 10)]
    assert [l["status"] for l in leg_text_rows(spark, rows, clock_t=40)["a"]] == ["LANDED"]


def test_taxi_speed_boundary(spark):
    rows = [gnd("a", 0), air("a", 10, speed=99.9), air("a", 20, speed=99.9),   # 低于 100：还在地面
            gnd("b", 0), air("b", 10, speed=100.0)]                             # 正好 100：起飞
    assert event_list(spark, rows) == [("b", "TAKEOFF", "ZRH", 10)]
    assert "a" not in leg_text_rows(spark, rows, clock_t=20)


def test_slow_record_beyond_5_km_stays_in_air(spark):
    # 离机场 5.1 km 的慢速记录（比如直升机）照常算空中；5.0 km 以内算地面
    rows = [air("a", 0, speed=50.0, dist=5.1), air("a", 10, speed=50.0, dist=5.1),
            air("b", 0, speed=50.0, dist=5.0), air("b", 10, speed=50.0, dist=5.0)]
    legs = leg_text_rows(spark, rows, clock_t=10)
    assert len(legs["a"]) == 1 and "b" not in legs


def test_missing_speed_keeps_reported_state(spark):
    rows = [air("a", 0, speed=None), air("a", 10, speed=None)]
    assert len(leg_text_rows(spark, rows, clock_t=10)["a"]) == 1


def test_landing_found_by_slowing_down(spark):
    # 应答机一直报"空中"：减速到 100 km/h 以下那一条按落地算
    rows = [air("a", 0, speed=250.0, dist=3.0), air("a", 10, speed=180.0, dist=1.5),
            air("a", 20, speed=60.0, dist=1.0), air("a", 30, speed=20.0, dist=0.8)]
    assert event_list(spark, rows) == [("a", "LANDING", "ZRH", 20)]
    leg = leg_text_rows(spark, rows, clock_t=30)["a"][0]
    assert leg["status"] == "LANDED" and leg["last"] == utc(10) and leg["end"] == utc(20)


# ================================================================
#  航段切分
# ================================================================
def test_gap_19_minutes_keeps_one_leg(spark):
    legs = leg_text_rows(spark, [air("a", 0), air("a", 19 * MIN)], clock_t=19 * MIN)
    assert len(legs["a"]) == 1
    assert legs["a"][0]["status"] == "IN_AIR"
    assert legs["a"][0]["last"] == utc(19 * MIN)


def test_gap_exactly_20_minutes_keeps_one_leg(spark):
    legs = leg_text_rows(spark, [air("a", 0), air("a", 20 * MIN)], clock_t=20 * MIN)
    assert len(legs["a"]) == 1


def test_gap_21_minutes_splits(spark):
    legs = leg_text_rows(spark, [air("a", 0), air("a", 21 * MIN)], clock_t=21 * MIN)["a"]
    assert [l["status"] for l in legs] == ["LOST", "IN_AIR"]
    assert legs[0]["end"] == utc(0)                          # LOST 的结束时间取最后一条空中记录
    assert legs[1]["departure_kind"] == "OUTSIDE"            # 之前 20 分钟内没有记录
    assert legs[1]["leg_id"] == f"a_{T0 + 21 * MIN}"


def test_leave_coverage_and_come_back(spark):
    # 飞出采集范围 15 分钟又回来：同一个航段；离开 25 分钟：两个航段
    short = [air("a", 0), air("a", 60), air("a", 16 * MIN)]
    long = [air("b", 0), air("b", 60), air("b", 26 * MIN)]
    legs = leg_text_rows(spark, short + long, clock_t=26 * MIN)
    assert len(legs["a"]) == 1
    assert [l["status"] for l in legs["b"]] == ["LOST", "IN_AIR"]
    assert legs["b"][0]["end"] == utc(60)


def test_land_then_take_off_again(spark):
    rows = ([air("a", 0, alt=900.0), air("a", 10, alt=300.0), gnd("a", 20)]   # 落地 ZRH
            + [gnd("a", 20 + 60 * k) for k in range(1, 41)]                   # 地面 40 分钟
            + [air("a", 2440, iata="ZRH", alt=200.0), air("a", 2450, alt=800.0)])  # 起飞
    legs = leg_text_rows(spark, rows, clock_t=2450)["a"]
    assert [l["status"] for l in legs] == ["LANDED", "IN_AIR"]
    first, second = legs
    assert first["arrival_iata"] == "ZRH" and first["end"] == utc(20)
    assert first["last"] == utc(10)                          # 地面记录不更新最后出现时间
    assert first["max_altitude_m"] == 900.0
    assert second["departure_kind"] == "TAKEOFF" and second["departure_iata"] == "ZRH"
    assert second["start"] == utc(2440)


def test_undetected_landing_times_out(spark):
    # 落在维表外的机场：翻转后离最近机场 12 km，不算落地；之后一直有地面记录，航段也不延续
    rows = ([air("a", 0, iata="CGN", dist=20.0), air("a", 10, iata="CGN", dist=12.5),
             gnd("a", 20, iata="CGN", dist=12.0)]
            + [gnd("a", 20 + 60 * k, iata="CGN", dist=12.0) for k in range(1, 31)])
    legs = leg_text_rows(spark, rows, clock_t=20 + 30 * MIN)["a"]
    assert len(legs) == 1
    leg = legs[0]
    assert leg["status"] == "LOST" and leg["end"] == utc(10) and leg["arrival_iata"] is None


def test_undetected_landing_still_in_air_within_20_minutes(spark):
    rows = [air("a", 0, dist=12.0), gnd("a", 10, dist=12.0), gnd("a", 10 * MIN, dist=12.0)]
    legs = leg_text_rows(spark, rows, clock_t=10 * MIN)["a"]
    assert [l["status"] for l in legs] == ["IN_AIR"]         # 已知限制：20 分钟后才会变 LOST


def test_undetected_takeoff(spark):
    # 地面记录之后隔了 90 秒才出现空中记录：翻转不算，但要开航段，起飞机场记未知
    rows = [gnd("a", 0), gnd("a", 10), air("a", 100), air("a", 110)]
    legs = leg_text_rows(spark, rows, clock_t=110)["a"]
    assert len(legs) == 1
    assert legs[0]["departure_kind"] == "UNDETECTED" and legs[0]["departure_iata"] is None


def test_ground_record_older_than_20_minutes_means_outside(spark):
    rows = [gnd("a", 0), air("a", 21 * MIN)]
    legs = leg_text_rows(spark, rows, clock_t=21 * MIN)["a"]
    assert legs[0]["departure_kind"] == "OUTSIDE"


def test_first_seen_in_air(spark):
    legs = leg_text_rows(spark, [air("a", 0), air("a", 10)], clock_t=10)["a"]
    assert legs[0]["departure_kind"] == "OUTSIDE" and legs[0]["status"] == "IN_AIR"


def test_takeoff_event_closes_open_leg(spark):
    # 落地翻转被跳过（间隔 90 秒），地面停 3 分钟后正常起飞：旧航段记 LOST，新航段从起飞开始。
    # 旧航段最后一条空中记录低空下降、离 ZRH 1 km，被关掉时同样判丢失原因
    rows = [air("a", 0, alt=300.0, phase="DESCENDING"), gnd("a", 90), gnd("a", 270),
            air("a", 280), air("a", 290)]
    legs = leg_text_rows(spark, rows, clock_t=290)["a"]
    assert [l["status"] for l in legs] == ["LOST", "IN_AIR"]
    assert legs[0]["end"] == utc(0)
    assert legs[0]["lost_reason"] == "SUSPECTED_LANDING" and legs[0]["last_airport_iata"] == "ZRH"
    assert legs[1]["departure_kind"] == "TAKEOFF" and legs[1]["start"] == utc(280)


def test_both_flips_missed_with_short_stop_merge(spark):
    # 已知限制：落地和起飞都没判出来、地面停留不到 20 分钟，两段飞行并成一个航段
    rows = [air("a", 0), gnd("a", 90), gnd("a", 5 * MIN), air("a", 7 * MIN)]
    legs = leg_text_rows(spark, rows, clock_t=7 * MIN)["a"]
    assert len(legs) == 1 and legs[0]["status"] == "IN_AIR"


def test_last_leg_lost_after_20_minutes_of_data_time(spark):
    rows = [air("a", 0), air("a", 10)]
    assert leg_text_rows(spark, rows, clock_t=10 + 20 * MIN)["a"][0]["status"] == "IN_AIR"
    assert leg_text_rows(spark, rows, clock_t=11 + 20 * MIN)["a"][0]["status"] == "LOST"


def test_last_position_and_max_altitude(spark):
    rows = [air("a", 0, alt=3000.0, lat=47.0, callsign="X1"),
            air("a", 10, alt=5000.0, lat=47.1, callsign="X1"),
            air("a", 20, alt=4000.0, lat=47.2, callsign="X2", phase="DESCENDING", iata="FRA", dist=40.0)]
    leg = leg_text_rows(spark, rows, clock_t=20)["a"][0]
    assert leg["max_altitude_m"] == 5000.0
    assert leg["last_altitude_m"] == 4000.0 and leg["callsign"] == "X2"
    assert (leg["last_flight_phase"], leg["last_airport_iata"], leg["last_airport_dist_km"]) == (
        "DESCENDING", "FRA", 40.0)


# ================================================================
#  丢失原因（按最后一条空中记录）
# ================================================================
def test_suspected_landing_boundaries(spark):
    # 离机场 ≤ 10 km、低于 1,000 m、在下降，三条都满足才算疑似落地未识别
    rows = [air("a", 0, dist=10.0, alt=999.0, phase="DESCENDING"),
            air("b", 0, dist=10.1, alt=999.0, phase="DESCENDING"),    # 远了 0.1 km
            air("c", 0, dist=10.0, alt=1000.0, phase="DESCENDING"),   # 正好 1,000 m
            air("d", 0, dist=10.0, alt=999.0, phase="CRUISING"),      # 没在下降
            air("e", 0, dist=10.0, alt=999.0, phase="CLIMBING")]      # 刚起飞就没信号
    assert lost_reasons(spark, rows, clock_t=21 * MIN) == {
        "a": ["SUSPECTED_LANDING"], "b": ["SIGNAL_LOST"], "c": ["SIGNAL_LOST"],
        "d": ["SIGNAL_LOST"], "e": ["SIGNAL_LOST"]}


def test_exited_near_edge(spark):
    # 2026-10-17 起的范围是北纬 43–52°、东经 2.5–12.5°，离边界 ≤ 20 km 算飞出范围
    rows = [air("a", 0, lat=51.85, lon=8.0, dist=80.0),     # 离北边界约 16.7 km
            air("b", 0, lat=51.80, lon=8.0, dist=80.0),     # 约 22.2 km
            air("c", 0, lat=47.0, lon=12.3, dist=80.0)]     # 离东边界约 15.2 km（经度差还要乘 cos 纬度）
    assert lost_reasons(spark, rows, clock_t=21 * MIN) == {
        "a": ["EXITED"], "b": ["SIGNAL_LOST"], "c": ["EXITED"]}


def test_exited_uses_coverage_of_that_time(spark):
    # 同一个位置（北纬 49.9°）：7 月离当时的北边界 50° 约 11 km；10 月离 52° 约 234 km
    rows = [air("a", JULY, lat=49.9, lon=8.0, dist=80.0),
            air("b", 0, lat=49.9, lon=8.0, dist=80.0)]
    assert lost_reasons(spark, rows, clock_t=21 * MIN) == {"a": ["EXITED"], "b": ["SIGNAL_LOST"]}


def test_suspected_landing_comes_before_exited(spark):
    # 7 月范围的西边界是 5°E，里昂机场在 5.08°E：在它附近低空下降时既靠边又像落地，按疑似落地未识别算
    rows = [air("a", JULY, lat=45.73, lon=5.05, iata="LYS", dist=3.0, alt=500.0, phase="DESCENDING")]
    assert lost_reasons(spark, rows, clock_t=21 * MIN) == {"a": ["SUSPECTED_LANDING"]}


def test_lost_reason_only_on_lost_legs(spark):
    rows = [air("a", 0), gnd("a", 10),                       # LANDED
            air("b", 0), air("b", 10)]                       # IN_AIR
    assert lost_reasons(spark, rows, clock_t=10) == {"a": [None], "b": [None]}


# ================================================================
#  流式分批 = 批量重算；重放不改变结果
# ================================================================
def scenario():
    """一段约 80 分钟、8 架飞机的数据，覆盖上面所有情况，按 10 秒一张快照排列。"""
    rows = []
    # a：在 ZRH 落地，地面 40 分钟后再起飞，爬升中信号断了（信号丢失）
    rows += [air("a", t, alt=1500.0 - t) for t in range(0, 300, 10)]
    rows += [gnd("a", t) for t in range(300, 2700, 30)]
    rows += [air("a", t, alt=100.0 + t - 2700, phase="CLIMBING") for t in range(2700, 3300, 10)]
    # b：在北边界附近飞出范围 25 分钟再回来，两段都是飞出范围
    rows += [air("b", t, iata="FRA", dist=80.0, alt=11000.0, lat=51.9) for t in range(0, 600, 10)]
    rows += [air("b", t, iata="FRA", dist=80.0, alt=10000.0, lat=51.9) for t in range(2100, 2400, 10)]
    # c：落在维表外的机场（CGN 12 km），之后一直有地面记录
    rows += [air("c", t, iata="CGN", dist=30.0 - t / 30) for t in range(0, 500, 10)]
    rows += [gnd("c", t, iata="CGN", dist=12.0) for t in range(500, 3500, 20)]
    # d：起飞翻转被跳过
    rows += [gnd("d", t) for t in range(1000, 1300, 10)]
    rows += [air("d", t, alt=500.0 + t - 1400) for t in range(1400, 2500, 10)]
    # e：落地翻转被跳过（疑似落地未识别），地面 3 分钟后正常起飞
    rows += [air("e", t, alt=800.0, phase="DESCENDING") for t in range(3000, 3300, 10)]
    rows += [gnd("e", t) for t in range(3390, 3570, 10)]
    rows += [air("e", t, alt=300.0) for t in range(3580, 4000, 10)]
    # f：一直在飞，跨越很多批，最后一直到结尾都在
    rows += [air("f", t, alt=9000.0 + (t % 700)) for t in range(0, 4800, 10)]
    # g：落地后滑行，每 7 条有一条标记跳成"空中"（25 km/h）：只有一次落地
    rows += [air("g", t, alt=1200.0 - 2 * t, phase="DESCENDING") for t in range(100, 400, 10)]
    rows += [air("g", t, speed=25.0) if (t // 10) % 7 == 0 else gnd("g", t, speed=25.0)
             for t in range(400, 1600, 10)]
    # h：应答机一直报"空中"，减速到 100 km/h 以下时算落地
    rows += [air("h", t, alt=900.0) for t in range(1000, 1200, 10)]
    rows += [air("h", t, alt=450.0, speed=speed)
             for t, speed in zip(range(1200, 1500, 10), [200.0, 150.0, 90.0] + [40.0] * 27)]
    return rows


def run_stream(spark, rows, batch_seconds, replay=(), events_only_replay=(), pass_bounds=False, threads=False):
    """模拟流式：按快照时间切成微批，每批调用 stream_step，再像 MERGE 一样写回内存里的表。

    replay：这些批处理两次（第一次完整写入后重放）。
    events_only_replay：这些批第一次只写入了起降事件、航段 MERGE 失败，然后重放。
    pass_bounds：像实时作业那样，本批范围由调用方用 batch_bounds 先算好再传进去。
    threads：像实时作业那样传 submit，让"上一个航段"和"翻转"在两个线程里同时算。
    """
    pool = ThreadPoolExecutor(max_workers=2) if threads else None

    def submit(fn):
        def run():
            # 新线程在 JVM 一侧没有当前会话，先设上（同 rt_pipeline.in_session），时间常量才按 UTC 解释
            spark._jvm.org.apache.spark.sql.SparkSession.setActiveSession(spark._jsparkSession)
            return fn()
        return pool.submit(run)

    def frozen(df):
        # 收集到 driver 再建成本地 DataFrame：计划很浅、统计按实际大小，也不带之前各批的血缘。
        # 用 persist 的话，缓存的执行计划会一批套一批地嵌进后面的计划里，几十批后计划字符串就把内存撑爆了
        return spark.createDataFrame(df.collect(), df.schema)

    all_states = frozen(states_df(spark, rows))
    times = sorted({r[1] for r in rows})
    edges = list(range(times[0], times[-1] + batch_seconds, batch_seconds)) + [times[-1] + 1]
    events, legs = compute_all(all_states.limit(0), "1970-01-01 00:00:00")
    events, legs = frozen(events), frozen(legs)

    def before(t):
        return F.col("event_time") < F.timestamp_seconds(F.lit(T0 + t))

    for i, (lo, hi) in enumerate(zip(edges, edges[1:])):
        batch = all_states.where(~before(lo) & before(hi))
        if batch.isEmpty():
            continue
        # 流式作业读到本批时，dwd_state 已经把它提交进状态明细了
        seen = all_states.where(before(hi))
        attempts = 2 if (i in replay or i in events_only_replay) else 1
        for attempt in range(attempts):
            bounds = batch.agg(*batch_bounds()).first() if pass_bounds else None
            ev, leg_rows, _ = stream_step(batch, seen, legs, cache=frozen, bounds=bounds,
                                          submit=submit if threads else None)
            # 起降事件：按主键只插入
            events = frozen(events.unionByName(ev).dropDuplicates(list(EVENT_KEY)))
            if attempt == 0 and i in events_only_replay:
                continue                                     # 模拟航段 MERGE 失败
            # 航段：按 leg_id 整行 upsert
            legs = frozen(legs.join(leg_rows.select("leg_id"), "leg_id", "left_anti")
                          .unionByName(leg_rows))
    if pool is not None:
        pool.shutdown()
    return events, legs


def batch_truth(spark, rows):
    events, legs = compute_all(states_df(spark, rows))
    return events_as_text(events), legs_as_text(legs)


@pytest.fixture(scope="module")
def truth(spark):
    return batch_truth(spark, scenario())


def test_stream_matches_batch(spark, truth):
    # 150 秒一批：落地、起飞、航段延续都会跨批，长航段跨越三十多批
    events, legs = run_stream(spark, scenario(), 150)
    assert events_as_text(events) == truth[0]
    assert legs_as_text(legs) == truth[1]


def test_stream_with_bounds_from_caller_matches_batch(spark, truth):
    # 实时作业在算"本批新行"的同一个作业里顺带算出范围，传给 stream_step，结果要和自己算的一样
    events, legs = run_stream(spark, scenario(), 300, pass_bounds=True)
    assert events_as_text(events) == truth[0]
    assert legs_as_text(legs) == truth[1]


def test_stream_with_threads_matches_batch(spark, truth):
    # 实时作业让"上一个航段"和"翻转"两步在两个线程里同时算，结果要和依次算一样
    events, legs = run_stream(spark, scenario(), 300, pass_bounds=True, threads=True)
    assert events_as_text(events) == truth[0]
    assert legs_as_text(legs) == truth[1]


def test_replayed_batches_change_nothing(spark, truth):
    # 600 秒一批：第 2 批完整写入后重放；第 3、5 批第一次只写入了事件，航段 MERGE 失败后重放
    events, legs = run_stream(spark, scenario(), 600, replay={2}, events_only_replay={3, 5})
    assert events_as_text(events) == truth[0]
    assert legs_as_text(legs) == truth[1]


def test_scenario_covers_every_status(truth):
    def values(col):
        return {row[LEG_COLUMNS.index(col)] for row in truth[1]}
    assert values("status") == {"IN_AIR", "LANDED", "LOST"}
    assert values("departure_kind") == {"TAKEOFF", "UNDETECTED", "OUTSIDE"}
    assert values("lost_reason") == {None, "SUSPECTED_LANDING", "EXITED", "SIGNAL_LOST"}
    # g 的滑行抖动、h 的减速落地：各自只有一次落地
    assert [(e[0], e[2]) for e in truth[0] if e[0] in ("g", "h")] == [("g", "LANDING"), ("h", "LANDING")]
    assert LEG_GAP_S == 20 * 60
