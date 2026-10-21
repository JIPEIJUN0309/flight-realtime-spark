"""
pg_sync 的测试：建表与只读账号、旧表结构自动替换、CSV 往返不丢值、微批 upsert 与同步序号、
事件只插入、全量对齐删掉湖里没有的行、数据时钟只往后走、进近口径（三类样本只有低空下降计入）、
原子性：并发读者看不到半批，而非事务写入会被探针抓到。

需要一个 PostgreSQL（测试会新建并在结束时删掉 PG_TEST_URL 指向的库，库名必须以 _test 结尾），
以及 classpath 上的 PostgreSQL JDBC 驱动（spark 容器里已有）。没设置 PG_TEST_URL 时跳过。
在 spark 容器里跑：
  docker exec -e PG_TEST_URL=jdbc:postgresql://postgres:5432/flightdb_test spark bash -c \\
      "cd /opt/spark/work-dir && python3 -m pytest -q tests/test_pg_sync.py"
"""

import os
import threading
from datetime import datetime, timezone

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)

from flight_legs import compute_all
from latest_position import LATEST_COLUMNS, latest_updates
from pg_sync import PgClient, ensure_schema, primary_key, sync, table_specs

PG_TEST_URL = os.getenv("PG_TEST_URL")
PG_TEST_USER = os.getenv("PG_TEST_USER", "flight")
PG_TEST_PASSWORD = os.getenv("PG_TEST_PASSWORD", "flight123")

pytestmark = pytest.mark.skipif(
    not PG_TEST_URL, reason="需要 PostgreSQL：设置 PG_TEST_URL，例如 jdbc:postgresql://postgres:5432/flightdb_test")

# 取过去的时间：处理延迟 = 写入时间 - kafka_ts，测试里要是正数
T0 = int(datetime(2026, 1, 15, 8, 0, tzinfo=timezone.utc).timestamp())
TS = "YYYY-MM-DD HH24:MI:SS"


def utc(t, micros=False):
    fmt = "%Y-%m-%d %H:%M:%S.%f" if micros else "%Y-%m-%d %H:%M:%S"
    return datetime.fromtimestamp(T0 + t, tz=timezone.utc).strftime(fmt)


STATE_FIELDS = StructType([
    StructField("icao24", StringType()),
    StructField("t", DoubleType()),               # 相对 T0 的秒数，可以带小数
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("on_ground", BooleanType()),
    StructField("squawk", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("altitude_m", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("near_airport_iata", StringType()),
    StructField("near_airport_name", StringType()),
    StructField("airport_dist_km", DoubleType()),
])


def st(icao24, t, callsign="SWR1", country="Switzerland", ground=False, squawk="1000", lat=47.4, lon=8.5,
       alt=1000.0, vr=0.0, iata="ZRH", name="Zurich Airport", dist=12.0):
    return (icao24, float(t), callsign, country, ground, squawk, lat, lon, alt, vr, iata, name, dist)


def states_df(spark, rows):
    """状态明细：最新状态和起降航段用到的列都有。"""
    df = spark.createDataFrame(rows, STATE_FIELDS)
    return (
        df.withColumn("event_time", F.timestamp_micros((F.col("t") * 1e6).cast("long") + T0 * 1_000_000))
        .withColumn("kafka_ts", F.col("event_time") + F.expr("INTERVAL 2 SECONDS"))
        .withColumn("speed_kmh", F.when(F.col("on_ground"), 20.0).otherwise(400.0))
        .withColumn("flight_phase", F.when(F.col("on_ground"), "GROUND").otherwise("CRUISING"))
        .withColumn("heading_deg", F.lit(90.0))
        .withColumn("heading_dir", F.lit("E"))
        .withColumn("near_airport_city", F.lit("Zurich"))
        .drop("t")
    )


def empty_latest(spark):
    return states_df(spark, []).withColumn("departed", F.lit(False)).select(*LATEST_COLUMNS)


def latest_rows(spark, rows, clock_t):
    return latest_updates(empty_latest(spark), states_df(spark, rows), utc(clock_t))


def fake(spark, spec, n, k):
    """按表结构造 n 行，第 k 批：主键随 id 唯一，其余值随 k 变化。"""
    cols = []
    for f in spec.fields:
        t = f.dataType
        if isinstance(t, StringType):
            c = F.concat(F.lit(f.name[:4]), F.col("id").cast("string"))
        elif isinstance(t, TimestampType):
            c = F.timestamp_seconds(F.lit(T0 + 10 * k) + F.col("id") % 7)
        elif isinstance(t, DateType):
            c = F.to_date(F.timestamp_seconds(F.lit(T0)))
        elif isinstance(t, DoubleType):
            c = (F.col("id") * 0.5 + k).cast("double")
        elif isinstance(t, BooleanType):
            c = F.col("id") % 2 == 0
        elif isinstance(t, LongType):
            c = F.col("id") + k
        elif isinstance(t, IntegerType):
            c = (F.col("id") + k).cast("int")
        else:
            raise TypeError(t)
        cols.append(c.alias(f.name))
    return spark.range(n).select(*cols)


@pytest.fixture(scope="module")
def pg(spark):
    base, db = PG_TEST_URL.rsplit("/", 1)
    if not db.endswith("_test"):
        pytest.fail(f"测试库名必须以 _test 结尾，免得删掉正式库：{db}")
    try:
        spark._jvm.org.postgresql.Driver()
    except Exception:  # noqa: BLE001 —— 驱动类不在时 py4j 抛的不是统一的异常
        pytest.skip("classpath 上没有 PostgreSQL JDBC 驱动")
    admin = PgClient(spark, url=f"{base}/postgres", user=PG_TEST_USER, password=PG_TEST_PASSWORD,
                     autocommit=True)
    admin.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
    admin.execute(f"CREATE DATABASE {db}")
    client = PgClient(spark, url=PG_TEST_URL, user=PG_TEST_USER, password=PG_TEST_PASSWORD)
    yield client
    client.close()
    admin.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
    admin.close()


@pytest.fixture(scope="module")
def specs(spark):
    return table_specs(states_df(spark, []))


@pytest.fixture
def fresh(pg, specs):
    """每个测试从空表开始。"""
    ensure_schema(pg, list(specs.values()), log=lambda _: None)
    with pg.transaction():
        for spec in specs.values():
            pg.execute(f"TRUNCATE {spec.name}")
        pg.execute("TRUNCATE rt_sync_log, rt_batch_log")
        pg.execute("UPDATE rt_sync_state SET seq = 0, kind = NULL, batch_id = NULL, data_clock = NULL, "
                   "latest_changed = NULL, legs_changed = NULL, events_changed = NULL")
    return specs


def state_row(pg):
    return pg.query(f"SELECT seq, kind, batch_id, to_char(data_clock AT TIME ZONE 'UTC', '{TS}'), "
                    "latest_changed, legs_changed, events_changed FROM rt_sync_state")[0]


def check(pg):
    return pg.query("SELECT seq_complete, no_newer_rows FROM rt_atomicity_check")[0]


# ================================================================
#  建表
# ================================================================
def test_schema_created_and_idempotent(spark, pg, specs):
    ensure_schema(pg, list(specs.values()), log=lambda _: None)
    ensure_schema(pg, list(specs.values()), log=lambda _: None)
    tables = {r[0] for r in pg.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema()")}
    assert {"ads_latest_position", "dwd_flight_leg", "dwd_airport_event", "rt_sync_state", "rt_sync_log",
            "rt_batch_log", "rt_current_position", "rt_atomicity_check"} <= tables
    assert primary_key(pg, "ads_latest_position") == ["icao24"]
    assert primary_key(pg, "dwd_airport_event") == ["event_time", "event_type", "icao24"]
    # 只读账号能读视图，不能写
    reader = PgClient(spark, url=PG_TEST_URL, user="grafana_ro", password="grafana_ro", autocommit=True)
    try:
        assert len(reader.query("SELECT count(*) FROM rt_current_position")) == 1
        with pytest.raises(Exception):
            reader.execute("DELETE FROM ads_latest_position")
    finally:
        reader.close()


def test_legacy_table_is_replaced(pg, specs):
    # 批链路以前用 JDBC 整表覆盖写的 ads_latest_position：没有主键，列也不一样
    with pg.transaction():
        pg.execute("DROP TABLE IF EXISTS ads_latest_position CASCADE")
        pg.execute("CREATE TABLE ads_latest_position (icao24 text, callsign text, event_time timestamp)")
        pg.execute("INSERT INTO ads_latest_position VALUES ('old', 'OLD1', now())")
    logs = []
    ensure_schema(pg, list(specs.values()), log=logs.append)
    assert len(logs) == 1 and "结构和代码不一致" in logs[0] and "主键" in logs[0]
    assert primary_key(pg, "ads_latest_position") == ["icao24"]
    assert pg.query("SELECT count(*) FROM ads_latest_position") == [("0",)]
    ensure_schema(pg, list(specs.values()), log=logs.append)
    assert len(logs) == 1                      # 结构对了就不再重建


# ================================================================
#  写入
# ================================================================
def test_round_trip_keeps_values(spark, pg, fresh):
    rows = [
        st("x1", 0.123456, callsign='A,B"C', name="Saint-Exupéry, Côte d'Azur", squawk="", country=None,
           lat=47.123456789, vr=-0.0001),
        st("x2", 1, callsign="  PAD  ", alt=None),
    ]
    sync(pg, [(fresh["latest"], latest_rows(spark, rows, 1))], "batch", clock=utc(1), batch_id=0)
    got = {r[0]: r[1:] for r in pg.query(
        "SELECT icao24, callsign, origin_country, squawk, near_airport_name, "
        "to_char(event_time AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US'), latitude::text, "
        "vertical_rate::text, altitude_m::text, departed::text FROM ads_latest_position")}
    assert got["x1"] == ('A,B"C', None, "", "Saint-Exupéry, Côte d'Azur", utc(0.123456, micros=True),
                         "47.123456789", "-0.0001", "1000", "false")
    assert got["x2"][0] == "  PAD  " and got["x2"][7] is None     # 首尾空格原样保留，空值还是 NULL


def test_batch_upsert_counts_and_clock(spark, pg, fresh):
    r1 = sync(pg, [(fresh["latest"], latest_rows(spark, [st("a", 0), st("b", 0)], 0)),
                   (fresh["legs"], fake(spark, fresh["legs"], 3, 0)),
                   (fresh["events"], fake(spark, fresh["events"], 2, 0))],
              "batch", clock=utc(0), batch_id=7,
              stats={"n_records": 2, "kafka_ts_min": utc(2, True), "kafka_ts_p50": utc(2, True)})
    assert r1["seq"] == 1 and r1["rows"] == {"ads_latest_position": 2, "dwd_flight_leg": 3,
                                             "dwd_airport_event": 2}
    assert state_row(pg) == ("1", "batch", "7", utc(0), "2", "3", "2")
    assert check(pg) == ("t", "t")

    # 第二批：a 有新位置，legs 整行更新；时钟往后走
    table = latest_rows(spark, [st("a", 0), st("b", 0)], 0)
    upd = latest_updates(table, states_df(spark, [st("a", 30, callsign="NEW")]), utc(30))
    sync(pg, [(fresh["latest"], upd), (fresh["legs"], fake(spark, fresh["legs"], 3, 1)),
              (fresh["events"], fake(spark, fresh["events"], 0, 1))], "batch", clock=utc(30), batch_id=8)
    assert state_row(pg) == ("2", "batch", "8", utc(30), "1", "3", "0")
    assert pg.query("SELECT callsign, sync_seq FROM ads_latest_position WHERE icao24 = 'a'") == [("NEW", "2")]
    assert pg.query("SELECT sync_seq FROM ads_latest_position WHERE icao24 = 'b'") == [("1",)]

    # 数据时钟只往后走：时钟更早的一批（例如重放）不会把它拨回去
    sync(pg, [(fresh["latest"], upd.limit(0))], "batch", clock=utc(10), batch_id=8)
    assert state_row(pg)[3] == utc(30)
    assert check(pg) == ("t", "t")

    lat = pg.query("SELECT n_records, latency_max_s >= 0, latency_p50_s >= 0 FROM rt_sync_log WHERE sync_seq = 1")
    assert lat == [("2", "t", "t")]


def test_events_insert_only_but_full_sync_corrects(spark, pg, fresh):
    events = fake(spark, fresh["events"], 1, 0)
    sync(pg, [(fresh["events"], events)], "batch", clock=utc(0), batch_id=0)
    changed = events.withColumn("airport_iata", F.lit("CHANGED"))
    sync(pg, [(fresh["events"], changed)], "batch", clock=utc(0), batch_id=1)
    assert pg.query("SELECT airport_iata FROM dwd_airport_event") == [("airp0",)]    # 微批只插入
    sync(pg, [(fresh["events"], changed)], "full", clock=utc(0))
    assert pg.query("SELECT airport_iata FROM dwd_airport_event") == [("CHANGED",)]  # 全量对齐以湖为准


def test_full_sync_mirrors_lake(spark, pg, fresh):
    sync(pg, [(fresh["legs"], fake(spark, fresh["legs"], 5, 0))], "batch", clock=utc(0), batch_id=0)
    # 湖里重建过航段，只剩 2 个：全量对齐后 PostgreSQL 也只剩 2 个，时钟按湖里的设置
    sync(pg, [(fresh["latest"], empty_latest(spark)), (fresh["legs"], fake(spark, fresh["legs"], 2, 3)),
              (fresh["events"], fake(spark, fresh["events"], 0, 0))], "full", clock=utc(5))
    assert pg.query("SELECT count(*) FROM dwd_flight_leg") == [("2",)]
    assert state_row(pg)[1:4] == ("full", None, utc(5))
    assert check(pg) == ("t", "t")


def test_current_view_counts_only_low_descending_as_approach(spark, pg, fresh):
    rows = [
        st("high", 0, alt=9000.0, vr=-5.0, dist=10.0),     # 高空下降路过
        st("level", 0, alt=1500.0, vr=0.0, dist=10.0),     # 低空平飞
        st("appr", 0, alt=1500.0, vr=-4.0, dist=10.0),     # 低空下降：进近
        st("far", 0, alt=1500.0, vr=-4.0, dist=31.0),      # 30 km 外
        st("dep", 0, alt=900.0, vr=6.0, dist=5.0),         # 离场
        st("nullvr", 0, alt=1500.0, vr=None, dist=10.0),   # 垂直速率为空：不计
        st("old", -31, alt=1500.0, vr=-4.0, dist=10.0),    # 31 秒没更新：不算"当前"
    ]
    sync(pg, [(fresh["latest"], latest_rows(spark, rows, 0))], "batch", clock=utc(0), batch_id=0)
    flags = dict((r[0], r[1:]) for r in pg.query(
        "SELECT icao24, is_approach::text, is_departure::text FROM rt_current_position"))
    assert flags == {"high": ("false", "false"), "level": ("false", "false"), "appr": ("true", "false"),
                     "far": ("false", "false"), "dep": ("false", "true"), "nullvr": ("false", "false")}
    assert pg.query("SELECT count(*) FILTER (WHERE is_approach) FROM rt_current_position "
                    "WHERE near_airport_iata = 'ZRH'") == [("1",)]


# ================================================================
#  原子性
# ================================================================
def test_probe_catches_half_written_batch(spark, pg, fresh):
    """反例：不在一个事务里写，探针能抓到。证明探针本身有效。"""
    sync(pg, [(fresh["latest"], latest_rows(spark, [st(f"a{i}", 0) for i in range(4)], 0))],
         "batch", clock=utc(0), batch_id=0)
    assert check(pg) == ("t", "t")
    with pg.transaction():
        # 下一批的行先提交了一半，状态行还没更新：读者看到了"更新的序号"，上一批的行数也对不上了
        pg.execute("UPDATE ads_latest_position SET sync_seq = 2 WHERE icao24 IN ('a0', 'a1')")
    assert check(pg) == ("f", "f")
    with pg.transaction():
        # 状态行先更新、行只写了一半：标着最新序号的行数对不上
        pg.execute("UPDATE rt_sync_state SET seq = 2, latest_changed = 4")
    assert check(pg) == ("f", "t")


def test_concurrent_reader_never_sees_half_batch(spark, pg, fresh):
    spec = fresh["latest"]
    stop = threading.Event()
    seen, bad, errors = set(), [], []

    def reader():
        client = PgClient(spark, url=PG_TEST_URL, user=PG_TEST_USER, password=PG_TEST_PASSWORD,
                          autocommit=True)
        try:
            while not stop.is_set():
                seq, complete, newer, n = client.query(
                    "SELECT seq, seq_complete, no_newer_rows, latest_rows FROM rt_atomicity_check")[0]
                seen.add(seq)
                if complete != "t" or newer != "t" or (seq != "0" and n == "0"):
                    bad.append((seq, complete, newer, n))
        except Exception as e:  # noqa: BLE001
            errors.append(e)
        finally:
            client.close()

    thread = threading.Thread(target=reader)
    thread.start()
    try:
        for k in range(12):
            sync(pg, [(spec, fake(spark, spec, 400, k)), (fresh["legs"], fake(spark, fresh["legs"], 200, k)),
                      (fresh["events"], fake(spark, fresh["events"], 20, k))],
                 "batch", clock=utc(10 * k), batch_id=k)
    finally:
        stop.set()
        thread.join()
    assert not errors and not bad
    assert len(seen) >= 3          # 读者确实和写入交错进行
