"""
alert_engine 的集成测试：用一个人造的服务层数据库验证每条规则和去重/恢复逻辑。
需要一个测试库（表结构由测试自己建），所以默认跳过：

    docker exec -e ALERT_TEST_DSN="host=postgres port=5432 dbname=flightdb_test user=flight password=flight123" \
        spark python3 -m pytest -q tests/test_alert_engine.py

库名必须以 _test 结尾：测试会先 DROP 再用这些表，指错了会把真数据删掉。
"""

import os
import time
from datetime import datetime, timezone

import pytest

psycopg2 = pytest.importorskip("psycopg2")

DSN = os.getenv("ALERT_TEST_DSN", "")
pytestmark = pytest.mark.skipif(
    not DSN, reason="设置 ALERT_TEST_DSN（库名以 _test 结尾）才跑")

TABLES = """
DROP TABLE IF EXISTS alert_event, dq_result, dwd_flight_leg, ads_latest_position,
                     rt_sync_state, rt_batch_log, rt_heartbeat CASCADE;
CREATE TABLE rt_batch_log (
    query_id text, batch_id bigint, trigger_at timestamptz, num_input_rows bigint,
    trigger_ms int, add_batch_ms int, compute_ms int, pg_ms int, delta_ms int, sync_seq bigint,
    PRIMARY KEY (query_id, batch_id)
);
CREATE TABLE rt_heartbeat (id int PRIMARY KEY, beat_at timestamptz);
CREATE TABLE rt_sync_state (
    id int PRIMARY KEY, seq bigint, kind text, batch_id bigint, data_clock timestamptz,
    synced_at timestamptz, latest_changed int, legs_changed int, events_changed int
);
CREATE TABLE ads_latest_position (
    icao24 text PRIMARY KEY, event_time timestamptz, callsign text, squawk text,
    departed boolean DEFAULT false
);
CREATE TABLE dwd_flight_leg (
    leg_id text PRIMARY KEY, icao24 text, status text, last_seen timestamptz
);
CREATE TABLE dq_result (
    rule_code text, level text, passed boolean, checked_at timestamptz, detail text
);
"""


@pytest.fixture()
def engine():
    if "_test" not in DSN:
        pytest.skip("ALERT_TEST_DSN 必须指向以 _test 结尾的库")
    conn = psycopg2.connect(DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(TABLES)
    from alert_engine import AlertEngine, DDL

    with conn.cursor() as cur:
        cur.execute(DDL)
    yield AlertEngine(conn, notify=False)
    conn.close()


def healthy(eng, pipeline_age=2, data_age=10):
    """服务层健康：刚跑过一批、数据时钟也在走、没有应急代码。不动 alert_event。"""
    with eng.conn.cursor() as cur:
        cur.execute("DELETE FROM rt_batch_log")
        cur.execute(
            "INSERT INTO rt_batch_log VALUES ('q', 1, now() - interval '{} seconds', "
            "160, 4000, 3500, 3000, 200, 1000, 1)".format(pipeline_age))
        cur.execute("DELETE FROM rt_sync_state")
        cur.execute(
            "INSERT INTO rt_sync_state (id, seq, data_clock, synced_at) "
            "VALUES (1, 1, now() - interval '{} seconds', now())".format(data_age))
        cur.execute("DELETE FROM rt_heartbeat")
        cur.execute("DELETE FROM ads_latest_position")
        cur.execute(
            "INSERT INTO ads_latest_position (icao24, event_time, callsign, squawk, departed) "
            "VALUES ('abc123', now() - interval '{} seconds', 'SWR1', '1000', false)"
            .format(data_age))


def keys_of(conditions):
    return {c["key"] for c in conditions}


def open_alerts(eng):
    with eng.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM alert_event WHERE cleared_at IS NULL")
        return cur.fetchone()[0]


def test_long_catchup_batch_is_not_a_stall(engine):
    """追积压时一个批可能跑几十秒：按"跑完的时间"算，不能按开始时间算。"""
    healthy(engine, pipeline_age=2)
    with engine.conn.cursor() as cur:
        cur.execute("DELETE FROM rt_batch_log")
        # 40 秒前开始、跑了 35 秒，5 秒前刚跑完 —— 作业是活的
        cur.execute(
            "INSERT INTO rt_batch_log VALUES ('q', 1, now() - interval '40 seconds', "
            "80000, 35000, 30000, 3000, 200, 800, 1)")
    assert "pipeline_stalled" not in keys_of(engine.evaluate())

    # 120 秒前开始、跑了 5 秒：115 秒没有进度，这才是停了
    with engine.conn.cursor() as cur:
        cur.execute("DELETE FROM rt_batch_log")
        cur.execute(
            "INSERT INTO rt_batch_log VALUES ('q', 1, now() - interval '120 seconds', "
            "80000, 5000, 4000, 800, 100, 300, 1)")
    assert "pipeline_stalled" in keys_of(engine.evaluate())


def test_healthy_system_has_no_alerts(engine):
    healthy(engine)
    assert engine.evaluate() == []


def test_pipeline_stalled(engine):
    healthy(engine, pipeline_age=40)
    assert "pipeline_stalled" in keys_of(engine.evaluate())


def test_collection_stalled_only_when_pipeline_alive(engine):
    healthy(engine, pipeline_age=2, data_age=120)
    assert "collection_stalled" in keys_of(engine.evaluate())

    # 管道自己也停了：只报流作业停止，不重复报采集中断
    healthy(engine, pipeline_age=40, data_age=120)
    keys = keys_of(engine.evaluate())
    assert "pipeline_stalled" in keys and "collection_stalled" not in keys


def test_batch_backlog_needs_three_slow_batches(engine):
    healthy(engine)
    with engine.conn.cursor() as cur:
        cur.execute("DELETE FROM rt_batch_log")
        for i in range(3):
            cur.execute(
                "INSERT INTO rt_batch_log VALUES ('q', {}, now(), 160, 9000, 5000, 4000, 300, 1500, {})"
                .format(i, i))
    assert "batch_backlog" in keys_of(engine.evaluate())

    # 只有两个慢批：还不够
    with engine.conn.cursor() as cur:
        cur.execute("DELETE FROM rt_batch_log WHERE batch_id = 0")
    assert "batch_backlog" not in keys_of(engine.evaluate())


def test_emergency_squawk_is_reported_once_per_leg(engine):
    healthy(engine)
    with engine.conn.cursor() as cur:
        cur.execute("UPDATE ads_latest_position SET squawk = '7700'")
        cur.execute("INSERT INTO dwd_flight_leg VALUES ('abc123_1', 'abc123', 'IN_AIR', now())")

    assert "emergency:abc123_1:7700" in keys_of(engine.evaluate())

    # 条件还在：只更新 last_seen，不新增、不重复推送
    engine.tick()
    with engine.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM alert_event WHERE alert_key LIKE 'emergency:%'")
        assert cur.fetchone()[0] == 1
    assert open_alerts(engine) == 1

    # 航段落地：应急告警不自动关闭，等人工确认（项目报告 M7）
    with engine.conn.cursor() as cur:
        cur.execute("UPDATE dwd_flight_leg SET status = 'LANDED'")
    engine.tick()
    assert open_alerts(engine) == 1

    # 人工确认后下一轮才标成恢复
    with engine.conn.cursor() as cur:
        cur.execute("UPDATE alert_event SET acknowledged_at = now() "
                    "WHERE alert_key = 'emergency:abc123_1:7700'")
    engine.tick()
    assert open_alerts(engine) == 0

    # 同一航段再次出现：不再推第二次（按 alert_key 查全部历史）
    with engine.conn.cursor() as cur:
        cur.execute("UPDATE dwd_flight_leg SET status = 'IN_AIR'")
    engine.tick()
    with engine.conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM alert_event WHERE alert_key = 'emergency:abc123_1:7700'")
        assert cur.fetchone()[0] == 1
    assert open_alerts(engine) == 0


def test_emergency_without_leg_falls_back_to_aircraft(engine):
    healthy(engine)
    with engine.conn.cursor() as cur:
        cur.execute("UPDATE ads_latest_position SET squawk = '7600'")
    keys = keys_of(engine.evaluate())
    assert any(k.startswith("emergency:abc123-") and k.endswith(":7600") for k in keys)


def test_alert_holds_until_condition_clears(engine):
    healthy(engine, pipeline_age=40)
    engine.tick()
    assert open_alerts(engine) == 1

    healthy(engine, pipeline_age=1)
    engine.tick()
    assert open_alerts(engine) == 0


def test_dq_strong_failure_raises_alert(engine):
    healthy(engine)
    with engine.conn.cursor() as cur:
        cur.execute(
            "INSERT INTO dq_result VALUES ('snapshot_completeness', 'strong', false, now(), '完整率 0.5')")
    conditions = engine.evaluate()
    assert "dq_strong_failed" in keys_of(conditions)
    detail = [c for c in conditions if c["key"] == "dq_strong_failed"][0]["detail"]
    assert "snapshot_completeness" in detail


def test_card_stale_needs_to_hold_for_a_minute(engine):
    healthy(engine, data_age=90)
    # 新鲜度已经超了，但持续时间不够：不报
    assert "card_stale" not in keys_of(engine.evaluate())
    # 把"第一次发现"往前推 61 秒，模拟条件已持续
    engine._sick_since["card_stale"] = time.time() - 61
    assert "card_stale" in keys_of(engine.evaluate())

def test_idle_heartbeat_keeps_pipeline_alive(engine):
    """采集停了（没有新批次），但作业还在写心跳：不能报"流作业停止"（项目报告 M1）。"""
    healthy(engine, pipeline_age=40)
    with engine.conn.cursor() as cur:
        cur.execute("INSERT INTO rt_heartbeat (id, beat_at) VALUES (1, now()) "
                    "ON CONFLICT (id) DO UPDATE SET beat_at = EXCLUDED.beat_at")
    assert "pipeline_stalled" not in keys_of(engine.evaluate())

    # 心跳也停了：这才是真的停了
    with engine.conn.cursor() as cur:
        cur.execute("UPDATE rt_heartbeat SET beat_at = now() - interval '60 seconds'")
    assert "pipeline_stalled" in keys_of(engine.evaluate())
