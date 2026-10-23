"""
OpenSky → Kafka 生产者（阶段一核心）

做的事：
  1. 定时（默认每 30 秒）调用 OpenSky 的 /states/all 接口
  2. 用 bounding box 限定区域（默认北纬 43–52°、东经 2.5–12.5°），90 平方度每次请求花 2 credits
  3. 把返回的二维数组解析成一条条结构化的航班记录（dict）
  4. 逐条写入 Kafka 的 flights topic

这是采集层。它和下游的 Spark 消费完全解耦——Producer 只管把数据可靠地
送进 Kafka，Spark 怎么消费是它自己的事。

★ 采集端可靠性（M1 补的三件事）
  1. 连 Kafka 失败会重试。Docker 或 Kafka 还没起来时，kafka-python 抛的不只是
     NoBrokersAvailable，还可能是 ValueError、OSError，以前这两种会让程序直接崩掉。
  2. 检查发送结果。producer.send() 是异步的，失败只会通过回调告诉你。以前没接回调，
     Kafka 中途挂掉时程序照样打印"已发送"，数据丢了也不知道。
  3. 固定节拍轮询。以前是"处理完再睡 POLL_INTERVAL 秒"，每轮实际间隔 = 请求耗时 + 间隔，
     10 秒一轮会变成 11–12 秒一轮，采集完整率（实收轮次 ÷ 应收轮次）先天就到不了 99%。
     现在按 0、10、20 秒……的节拍发请求，请求耗时不会累积。
"""

import os
import json
import time
from datetime import datetime, timezone
import signal
import sys

from dotenv import load_dotenv
load_dotenv()

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from token_manager import TokenManager

# ---------------- 配置：全部从环境变量读，绝不硬编码 ----------------
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET", "")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))  # 采集间隔（秒）

# 每轮最多等多久让消息全部写进 Kafka（秒）。超时按本轮写入失败处理，下一轮照常进行
FLUSH_TIMEOUT = 15

# Bounding box：默认北纬 43–52°、东经 2.5–12.5°（瑞士、德国中南部、法国东部、意大利北部）
# 面积 90 平方度，每次请求花 2 credits（≤25 平方度花 1，25–100 平方度花 2）
# 想换区域改这四个值即可（lamin/lomin/lamax/lomax = 纬度下/经度下/纬度上/经度上）
BBOX = {
    "lamin": float(os.getenv("BBOX_LAMIN", "43.0")),
    "lomin": float(os.getenv("BBOX_LOMIN", "2.5")),
    "lamax": float(os.getenv("BBOX_LAMAX", "52.0")),
    "lomax": float(os.getenv("BBOX_LOMAX", "12.5")),
}

OPENSKY_URL = "https://opensky-network.org/api/states/all"

# OpenSky 状态向量的字段顺序（按官方文档的下标）
STATE_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]


def parse_state(state: list) -> dict:
    """把一条状态向量（数组）转成带字段名的 dict。数组比字段名少时补 None。"""
    record = {}
    for i, name in enumerate(STATE_FIELDS):
        record[name] = state[i] if i < len(state) else None
    # callsign 常带尾随空格，清理一下
    if record.get("callsign"):
        record["callsign"] = record["callsign"].strip()
    return record


def create_kafka_producer() -> KafkaProducer:
    """创建 Kafka 生产者，带重试（等 Kafka 起来）。"""
    last_error = None
    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",          # 最强可靠性：等所有副本确认
                retries=3,
                linger_ms=200,       # 稍微攒一下批，提升吞吐
                # Kafka 不可用时，send() 最多阻塞 10 秒就报错（默认 60 秒），不拖住整轮
                max_block_ms=10_000,
            )
            log(f"[kafka] 已连接 {KAFKA_BOOTSTRAP}")
            return producer
        # NoBrokersAvailable 是 KafkaError 的子类。Docker 或 Kafka 容器根本没起来时，
        # 建连阶段还可能抛 ValueError（例如 "Invalid file object: None"）或 OSError
        except (KafkaError, ValueError, OSError) as e:
            last_error = e
            log(f"[kafka] 第 {attempt} 次连接失败（{type(e).__name__}），2 秒后重试…")
            time.sleep(2)
    raise RuntimeError(
        f"无法连接 Kafka（{KAFKA_BOOTSTRAP}）：{last_error!r}。"
        "请先启动 Docker Desktop，再确认 docker compose ps 里 kafka 是 healthy。"
    )


class SendStats:
    """一轮发送的结果统计。send() 是异步的，失败只能从回调里拿到。"""

    def __init__(self):
        self.sent = 0
        self.failed = 0
        self.last_error = None

    def on_error(self, exc):
        # 由 Kafka 客户端的后台 I/O 线程调用；主线程在 flush() 返回后才读这些计数
        self.failed += 1
        self.last_error = exc


def log(msg: str) -> None:
    """带 UTC 时间戳的日志：演练记录要按时间对账，裸 print 没法定位。"""
    print(f"[{datetime.now(timezone.utc):%H:%M:%S}] {msg}", flush=True)


def fetch_states(tokens: TokenManager) -> dict:
    """调用 OpenSky 拉一次数据。返回解析后的 JSON，失败返回 None。"""
    try:
        resp = requests.get(
            OPENSKY_URL,
            headers=tokens.auth_header(),
            params=BBOX,
            timeout=20,
        )
    except requests.RequestException as e:
        log(f"[opensky] 请求异常：{e}")
        return None

    # 429 = 额度用完。头里给了要等多久就真的等，否则主循环会每秒空转、日志刷屏
    if resp.status_code == 429:
        raw = resp.headers.get("X-Rate-Limit-Retry-After-Seconds", "")
        try:
            wait = int(raw)
        except (TypeError, ValueError):
            wait = 60
        wait = max(1, min(wait, 3600))
        log(f"[opensky] 额度用尽(429)，按 Retry-After 等待 {wait} 秒")
        time.sleep(wait)
        return None
    if resp.status_code == 400:
        log("[opensky] 400：通常是时间戳超过 1 小时限制，检查请求参数")
        return None
    if resp.status_code != 200:
        log(f"[opensky] 非预期状态码 {resp.status_code}")
        return None

    # 顺便打印剩余额度，便于监控配额
    remaining = resp.headers.get("X-Rate-Limit-Remaining")
    if remaining is not None:
        log(f"[opensky] 本次请求成功，剩余额度 {remaining}")

    try:
        return resp.json()
    except ValueError as e:  # 响应不是合法 JSON，比如传输中被截断
        log(f"[opensky] 响应解析失败：{e}")
        return None


def poll_once(round_no: int, tokens: TokenManager, producer: KafkaProducer) -> None:
    """拉一次快照，逐条写进 Kafka，并确认这一轮写入的结果。"""
    data = fetch_states(tokens)
    if not (data and data.get("states")):
        log(f"[round {round_no}] 本轮无数据（可能该区域此刻航班少或请求失败）")
        return

    snapshot_time = data.get("time")
    stats = SendStats()
    for state in data["states"]:
        record = parse_state(state)
        # 附上本次快照时间，供下游做事件时间用
        record["snapshot_time"] = snapshot_time
        # 用 icao24 当 key，保证同一架飞机进同一分区、顺序一致
        future = producer.send(KAFKA_TOPIC, key=record["icao24"], value=record)
        future.add_errback(stats.on_error)
        stats.sent += 1
    # 等这一轮的消息都有了结果（成功或失败）；超时会抛 KafkaTimeoutError
    producer.flush(timeout=FLUSH_TIMEOUT)

    if stats.failed:
        log(f"[round {round_no}] 警告：{stats.failed}/{stats.sent} 条写入 Kafka 失败，"
              f"最近一次错误：{stats.last_error!r}")
    else:
        log(f"[round {round_no}] 已发送 {stats.sent} 条航班记录 → Kafka")


def run():
    log("=" * 56)
    log("  OpenSky → Kafka 采集器启动")
    log(f"  区域(bbox): {BBOX}")
    log(f"  采集间隔  : {POLL_INTERVAL} 秒")
    log(f"  目标 topic: {KAFKA_TOPIC} @ {KAFKA_BOOTSTRAP}")
    log("=" * 56)

    tokens = TokenManager(CLIENT_ID, CLIENT_SECRET)
    producer = create_kafka_producer()

    # 优雅退出：Ctrl+C 时把缓冲刷完再退
    def shutdown(signum, frame):
        log("\n[exit] 收到停止信号，刷新缓冲并关闭…")
        try:
            producer.flush(timeout=FLUSH_TIMEOUT)
        except KafkaError as e:
            log(f"[exit] 还有消息没写进 Kafka：{e!r}")
        producer.close(timeout=5)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    round_no = 0
    next_tick = time.monotonic()
    while True:
        round_no += 1
        try:
            poll_once(round_no, tokens, producer)
        except KafkaError as e:
            # Kafka 中途不可用：这一轮算丢失（下游的采集完整率会体现），下一轮照常
            log(f"[round {round_no}] 写入 Kafka 失败（{type(e).__name__}: {e}），下一轮继续")

        # 固定节拍：下一轮按计划时间发出，不受这一轮耗时影响
        next_tick += POLL_INTERVAL
        delay = next_tick - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        else:
            # 这一轮耗时超过了一个间隔（比如请求超时）：不补发错过的轮次，从现在重新对齐
            next_tick = time.monotonic()


if __name__ == "__main__":
    run()
