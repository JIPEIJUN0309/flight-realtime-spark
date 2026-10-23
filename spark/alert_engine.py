"""
告警引擎：轮询服务层，把 P1/P2 告警写进 alert_event 并推送
==========================================================
规则来自 docs/requirements.md「告警规则」。引擎不读 Delta，只轮询 PostgreSQL 里
实时作业已经写好的表 —— 这样它和实时链路解耦，实时作业停了它照样能发现并报警。

  P1 流作业停止      rt_batch_log 超过 15 秒没有新批（3 个触发周期）
  P1 采集中断        rt_sync_state 的数据时钟 30 秒没往前走（管道还活着才算）
  P1 应急代码        ads_latest_position 出现 squawk 7500/7600/7700，同一航段同一代码只报一次
  P2 实时卡片超时    ads_latest_position 的新鲜度超过 30 秒并持续 1 分钟
  P2 微批积压        最近 3 个有数据的批耗时都超过 5 秒
  P2 质量强规则失败  dq_result 最近一次检查里有强规则没过

去重和冷却：同一个 alert_key 在未恢复前只记一行、只推一次；恢复后写 cleared_at；
30 分钟内同样的告警再出现不重复推送（应急代码除外，它每次都推，由人确认）。

跑法（常驻，容器里已经装了 psycopg2）：
  docker exec -d spark python3 /opt/spark/work-dir/jobs/alert_engine.py
  只跑一轮看看现在会不会报警（排查用）：
  docker exec spark python3 /opt/spark/work-dir/jobs/alert_engine.py --once

推送：设置环境变量 ALERT_WEBHOOK_URL（飞书/钉钉机器人）才推；不设就只写库和日志，
验收时看 alert_event 表和引擎日志即可。ALERT_WEBHOOK_KIND 取 feishu / dingtalk / generic。
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras

PG_DSN = os.getenv(
    "ALERT_PG_DSN",
    "host=postgres port=5432 dbname=flightdb user=flight password=flight123",
)
INTERVAL_S = int(os.getenv("ALERT_INTERVAL_S", "15"))
WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")
WEBHOOK_KIND = os.getenv("ALERT_WEBHOOK_KIND", "feishu")
COOLDOWN_MIN = int(os.getenv("ALERT_COOLDOWN_MIN", "30"))

# 阈值（和 requirements.md 的告警规则一致，改这里要同步文档）
PIPELINE_STALL_S = 15       # 流作业连续 3 个触发周期（5 秒 × 3）没有进度
COLLECT_STALL_S = 30        # 连续 30 秒（3 轮）没有新快照
CARD_STALE_S = 30.0         # 实时卡片新鲜度超过它……
CARD_STALE_HOLD_S = 60      # ……并持续这么久才报
BACKLOG_BATCHES = 3         # 连续几个批超时才报积压
# 阈值挂在"处理延迟"而不是触发间隔上：验收时 97.5% 的批都 ≥ 5 秒，按 5 秒报会一直成立，
# 值班会习惯性忽略这条告警。SLA 的处理延迟目标是 10 秒，所以连续 3 批超过 8 秒才报（M2）。
BACKLOG_MS = int(os.getenv("ALERT_BACKLOG_MS", "8000"))
DQ_RECENT_MIN = 30          # 只看最近半小时的质量检查结果

# 进程名模式：告警引擎和实时作业跑在同一个 spark 容器里，可以直接看进程在不在。
# 不设置（例如在容器外跑、或跑单测）就不做进程检查，只靠 rt_batch_log 的时间判断。
PIPELINE_PATTERN = os.getenv("ALERT_PIPELINE_PATTERN", "")

DDL = """
CREATE TABLE IF NOT EXISTS alert_event (
    id          bigserial PRIMARY KEY,
    alert_key   text NOT NULL,
    level       text NOT NULL,              -- P1 / P2
    title       text NOT NULL,
    detail      text,
    fired_at    timestamptz NOT NULL,
    last_seen   timestamptz NOT NULL,
    notified_at timestamptz,
    cleared_at  timestamptz
);
-- 应急告警要人工确认才关闭（项目报告 M7）
ALTER TABLE alert_event ADD COLUMN IF NOT EXISTS acknowledged_at timestamptz;
-- 实时作业的空闲心跳：没有新数据时也要能证明"作业还活着"（项目报告 M1）
CREATE TABLE IF NOT EXISTS rt_heartbeat (
    id      int PRIMARY KEY,
    beat_at timestamptz NOT NULL
);
-- 同一个 alert_key 同时只能有一个未恢复的告警
CREATE UNIQUE INDEX IF NOT EXISTS alert_event_open
    ON alert_event (alert_key) WHERE cleared_at IS NULL;
CREATE INDEX IF NOT EXISTS alert_event_fired ON alert_event (fired_at DESC);
"""


def log(msg: str) -> None:
    """带 UTC 时间戳的日志：演练和对账都要按时间定位，裸 print 对不上。"""
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def connect():
    conn = psycopg2.connect(PG_DSN, connect_timeout=10)
    conn.autocommit = True
    return conn


class AlertEngine:
    def __init__(self, conn, notify: bool = True):
        self.conn = conn
        self.notify = notify
        self._sick_since = {}     # watch_key → 第一次发现条件成立的时间（持续型规则用）

    # ---------------- 查库辅助 ----------------
    def _scalar(self, sql: str):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
        return row[0] if row else None

    def _rows(self, sql: str):
        with self.conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    # ---------------- 规则 ----------------
    def pipeline_age_s(self):
        """实时作业还活着吗：取"最近一批跑完"和"空闲心跳"里更新的那个。

        两个坑（项目报告 M1 / L1）：
        - Spark 3.5 没有新数据时只回调 onQueryIdle，rt_batch_log 不写新行；采集一停，
          只按批次时间判断就会把"采集中断"误报成"流作业停止"。
        - 删 checkpoint 重启后 batch_id 从 0 开始，按 batch_id 排序会取到旧行；按
          trigger_at 才是真正的"最近"。

        用"开始时间 + 耗时"而不是开始时间：追积压时一批要跑十几秒，只看开始时间会把
        正常的长批误判成作业停了。
        """
        batch = self._scalar(
            "SELECT EXTRACT(EPOCH FROM now() "
            "  - (trigger_at + trigger_ms * interval '1 millisecond'))::float "
            "FROM rt_batch_log ORDER BY trigger_at DESC LIMIT 1")
        beat = None
        try:
            beat = self._scalar(
                "SELECT EXTRACT(EPOCH FROM now() - beat_at)::float "
                "FROM rt_heartbeat WHERE id = 1")
        except psycopg2.errors.UndefinedTable:
            self.conn.rollback()      # 还没启动过实时作业，没有心跳表
        ages = [a for a in (batch, beat) if a is not None]
        return min(ages) if ages else None

    def collection_age_s(self):
        return self._scalar(
            "SELECT EXTRACT(EPOCH FROM now() - data_clock)::float FROM rt_sync_state WHERE id = 1")

    def pipeline_alive(self):
        """实时作业的进程还在不在。只在容器里、配了进程名模式时可用；否则返回 None。"""
        if not PIPELINE_PATTERN:
            return None
        try:
            result = subprocess.run(["pgrep", "-f", PIPELINE_PATTERN],
                                    capture_output=True, timeout=5)
            return result.returncode == 0
        except (FileNotFoundError, OSError, subprocess.SubprocessError):
            return None

    def evaluate(self):
        """返回当前成立的告警列表。每条 {key, level, title, detail}。"""
        conditions = []
        pipeline_age = self.pipeline_age_s()
        alive = self.pipeline_alive()
        # 容器里能看进程就以进程为准：没有数据时不会产生新批次，只看批的时间会把
        # "采集停了"误判成"作业停了"。在容器外跑（或没配进程名）时退回看批的时间。
        if alive is not None:
            stalled = not alive
        else:
            stalled = pipeline_age is not None and pipeline_age > PIPELINE_STALL_S
        pipeline_ok = not stalled

        if stalled:
            detail = ("实时作业进程已经不在了" if alive is False
                      else f"实时作业 {pipeline_age:.0f} 秒没有跑完一个批"
                           f"（阈值 {PIPELINE_STALL_S} 秒）")
            conditions.append(dict(
                key="pipeline_stalled", level="P1", title="流作业停止", detail=detail))

        collect_age = self.collection_age_s()
        # 管道本身停了就不再报采集中断，否则一次故障会响两次
        if pipeline_ok and collect_age is not None and collect_age > COLLECT_STALL_S:
            conditions.append(dict(
                key="collection_stalled", level="P1", title="采集中断",
                detail=f"数据时钟 {collect_age:.0f} 秒没有前进（阈值 {COLLECT_STALL_S} 秒），"
                       "Kafka 没有新快照进来",
            ))

        backlog = self._rows(
            "SELECT batch_id, trigger_ms FROM rt_batch_log "
            "WHERE num_input_rows > 0 AND trigger_at > now() - interval '2 minutes' "
            "ORDER BY trigger_at DESC LIMIT "
            f"{BACKLOG_BATCHES}")
        if len(backlog) == BACKLOG_BATCHES and all(ms > BACKLOG_MS for _, ms in backlog):
            conditions.append(dict(
                key="batch_backlog", level="P2", title="微批积压",
                detail="最近 {} 个批耗时 {} ms，都超过触发间隔 {} ms"
                       .format(BACKLOG_BATCHES,
                               "/".join(str(ms) for _, ms in backlog), BACKLOG_MS),
            ))

        freshness = self._scalar(
            "SELECT EXTRACT(EPOCH FROM now() - max(event_time))::float FROM ads_latest_position")
        stale = freshness is not None and freshness > CARD_STALE_S
        if stale:
            since = self._sick_since.setdefault("card_stale", time.time())
            held = time.time() - since
            if held >= CARD_STALE_HOLD_S:
                conditions.append(dict(
                    key="card_stale", level="P2", title="实时卡片超时",
                    detail=f"新鲜度 {freshness:.0f} 秒（阈值 {CARD_STALE_S:.0f} 秒），"
                           f"已持续 {held:.0f} 秒",
                ))
        else:
            self._sick_since.pop("card_stale", None)

        for icao24, callsign, squawk, event_time, leg_id, leg_count in self._rows(
            "SELECT p.icao24, p.callsign, p.squawk, p.event_time, "
            "       (SELECT l.leg_id FROM dwd_flight_leg l "
            "         WHERE l.icao24 = p.icao24 AND l.status = 'IN_AIR' "
            "         ORDER BY l.last_seen DESC LIMIT 1) AS leg_id, "
            "       (SELECT count(*) FROM dwd_flight_leg l WHERE l.icao24 = p.icao24) AS leg_count "
            "FROM ads_latest_position p "
            "WHERE p.squawk IN ('7500', '7600', '7700') AND NOT p.departed"
        ):
            # 航段还在飞：按航段去重，落地后这条告警自然恢复。
            # 一架飞机一条航段记录都没有（刚上线、还没切出航段）：退回按飞机+日期去重，
            # 否则这种应急代码反而漏报。已经有航段但都不在飞，说明落地了，不再报。
            if leg_id:
                subject = leg_id
            elif leg_count == 0:
                subject = f"{icao24}-{str(event_time)[:10]}"
            else:
                continue
            conditions.append(dict(
                key=f"emergency:{subject}:{squawk}", level="P1", title="应急代码",
                detail=f"{callsign}（{icao24}）发出 squawk {squawk}，航段 {leg_id or '未切出'}",
            ))

        dq = []
        try:
            dq = self._rows(
                "SELECT rule_code, detail, checked_at FROM dq_result "
                "WHERE level = 'strong' AND NOT passed "
                f"AND checked_at > now() - interval '{DQ_RECENT_MIN} minutes' "
                "ORDER BY checked_at DESC")
        except psycopg2.errors.UndefinedTable:
            # 还没跑过 dq_checks 时表不存在，不算故障
            self.conn.rollback()
        if dq:
            codes = sorted({row[0] for row in dq})
            conditions.append(dict(
                key="dq_strong_failed", level="P2", title="质量强规则失败",
                detail=f"最近 {DQ_RECENT_MIN} 分钟有强规则未通过：{', '.join(codes)}；"
                       "这一段 ADS 不发布",
            ))

        return conditions

    # ---------------- 记录与推送 ----------------
    def fire(self, cond):
        """没有未恢复的同名告警就插入一行；需要推送时推送。返回是否新告警。"""
        key = cond["key"]
        emergency = key.startswith("emergency:")
        with self.conn.cursor() as cur:
            # 应急告警按 alert_key 查**全部历史**：同一航段同一个代码只推一次，
            # 飞机短暂失联再出现也不重复推（项目报告 M7）。普通告警只看未恢复的。
            cur.execute(
                "SELECT id FROM alert_event WHERE alert_key = %s"
                + ("" if emergency else " AND cleared_at IS NULL")
                + " ORDER BY id DESC LIMIT 1", (key,))
            if cur.fetchone():
                cur.execute("UPDATE alert_event SET last_seen = now() "
                            "WHERE alert_key = %s AND cleared_at IS NULL", (key,))
                return False

            # 冷却：同样的告警 30 分钟内刚恢复过，就不再推一次，但仍然记录
            cooled = False
            if not key.startswith("emergency:"):
                cur.execute(
                    "SELECT 1 FROM alert_event WHERE alert_key = %s "
                    "AND cleared_at > now() - interval '%s minutes' LIMIT 1",
                    (key, COOLDOWN_MIN))
                cooled = cur.fetchone() is not None

            should_notify = self.notify and not cooled
            cur.execute(
                "INSERT INTO alert_event (alert_key, level, title, detail, fired_at, "
                "                        last_seen, notified_at) "
                "VALUES (%s, %s, %s, %s, now(), now(), %s) RETURNING id",
                (key, cond["level"], cond["title"], cond["detail"],
                 datetime.now(timezone.utc) if should_notify else None))
            alert_id = cur.fetchone()[0]

        text = (f"[{cond['level']}] {cond['title']}\n{cond['detail']}\n"
                f"告警编号 {alert_id}，时间 {datetime.now(timezone.utc):%Y-%m-%d %H:%M:%S} UTC")
        log(f"[alert] 新告警 {cond['level']} {cond['title']}：{cond['detail']}")
        if should_notify:
            send_webhook(text)
        return True

    def clear_stale(self, active_keys):
        """这一轮没再出现的告警，把未恢复的行标成已恢复。"""
        with self.conn.cursor() as cur:
            cur.execute("SELECT alert_key FROM alert_event WHERE cleared_at IS NULL")
            open_keys = [row[0] for row in cur.fetchall()]
            # 应急告警不自动关闭：要人工确认（--ack），否则飞机短暂失联再出现
            # 就会对同一航段重复推送（项目报告 M7）。
            gone = [k for k in open_keys
                    if k not in active_keys and not k.startswith("emergency:")]
            for key in gone:
                cur.execute("UPDATE alert_event SET cleared_at = now() "
                            "WHERE alert_key = %s AND cleared_at IS NULL", (key,))
                log(f"[alert] 已恢复：{key}")

    def tick(self):
        conditions = self.evaluate()
        for cond in conditions:
            self.fire(cond)
        self.clear_stale({c["key"] for c in conditions})
        self.close_acknowledged()
        return conditions

    def close_acknowledged(self):
        """人工确认过的告警（应急代码），下一轮标成已恢复。"""
        with self.conn.cursor() as cur:
            cur.execute("UPDATE alert_event SET cleared_at = acknowledged_at "
                        "WHERE acknowledged_at IS NOT NULL AND cleared_at IS NULL")


def send_webhook(text: str) -> None:
    """推送到飞书/钉钉机器人。没配 URL 就只打日志。"""
    if not WEBHOOK_URL:
        log("[alert] 未配置 ALERT_WEBHOOK_URL，只记录不推送")
        return
    if WEBHOOK_KIND == "feishu":
        payload = {"msg_type": "text", "content": {"text": text}}
    elif WEBHOOK_KIND == "dingtalk":
        payload = {"msgtype": "text", "text": {"content": text}}
    else:
        payload = {"title": "flight-realtime-spark 告警", "text": text}

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        WEBHOOK_URL, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", "replace")
        log(f"[alert] 推送完成：HTTP {resp.status} {body[:120]}")
    except Exception as e:  # noqa: BLE001 —— 推送失败不能把引擎带崩，下一轮还会再试
        log(f"[alert] 推送失败：{e!r}")


def main() -> None:
    p = argparse.ArgumentParser(description="告警引擎：轮询服务层，写 alert_event 并推送")
    p.add_argument("--once", action="store_true", help="只检查一轮就退出（排查用）")
    p.add_argument("--no-notify", action="store_true", help="只写库，不推送")
    p.add_argument("--ack", type=int, metavar="ID",
                   help="人工确认一条告警（应急告警靠它关闭，见 runbook）")
    args = p.parse_args()

    conn = connect()
    if args.ack:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute("UPDATE alert_event SET acknowledged_at = now() "
                        "WHERE id = %s AND cleared_at IS NULL", (args.ack,))
            log(f"[alert] 已确认告警 {args.ack}（{cur.rowcount} 行）")
        return
    with conn.cursor() as cur:
        cur.execute(DDL)
    engine = AlertEngine(conn, notify=not args.no_notify)

    if args.once:
        conditions = engine.tick()
        log(f"[alert] 本轮 {len(conditions)} 条告警")
        for c in conditions:
            log(f"  {c['level']} {c['title']}：{c['detail']}")
        return

    log(f"[alert] 告警引擎启动，每 {INTERVAL_S} 秒检查一次；"
          f"推送={'开' if WEBHOOK_URL else '关（没配 webhook）'}")
    while True:
        try:
            engine.tick()
        except Exception as e:  # noqa: BLE001 —— 数据库暂时不可用时重连，不让引擎退出
            log(f"[alert] 本轮失败：{e!r}，10 秒后重连")
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass
            time.sleep(10)
            # 重连本身也可能失败（数据库还没起来）。这里要再兜一层，
            # 否则异常会冒出循环、把整个引擎带退出 —— PG 宕机演练时就踩到过。
            try:
                conn = connect()
                engine.conn = conn
            except Exception as e2:  # noqa: BLE001
                log(f"[alert] 重连失败：{e2!r}，10 秒后再试")
                time.sleep(10)
            continue
        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    main()
