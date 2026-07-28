"""
OpenSky → Kafka 生产者（阶段一核心）

做的事：
  1. 定时（默认每 30 秒）调用 OpenSky 的 /states/all 接口
  2. 用 bounding box 限定区域（默认欧洲，飞机最多、覆盖最好），每次只花 1 credit
  3. 把返回的二维数组解析成一条条结构化的航班记录（dict）
  4. 逐条写入 Kafka 的 flights topic

这是采集层。它和下游的 Spark 消费完全解耦——Producer 只管把数据可靠地
送进 Kafka，Spark 怎么消费是它自己的事。
"""

import os
import json
import time
import signal
import sys

from dotenv import load_dotenv
load_dotenv()

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

from token_manager import TokenManager

# ---------------- 配置：全部从环境变量读，绝不硬编码 ----------------
CLIENT_ID = os.getenv("OPENSKY_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("OPENSKY_CLIENT_SECRET", "")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flights")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))  # 采集间隔（秒）

# Bounding box：默认框住欧洲（航班密度最高，OpenSky 在欧洲覆盖最好）
# 面积 ≤ 25 平方度时每次请求只花 1 credit
# 想换区域改这四个值即可（lamin/lomin/lamax/lomax = 纬度下/经度下/纬度上/经度上）
BBOX = {
    "lamin": float(os.getenv("BBOX_LAMIN", "45.0")),
    "lomin": float(os.getenv("BBOX_LOMIN", "5.0")),
    "lamax": float(os.getenv("BBOX_LAMAX", "50.0")),
    "lomax": float(os.getenv("BBOX_LOMAX", "10.0")),
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
    for attempt in range(1, 31):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",          # 最强可靠性：等所有副本确认
                retries=3,
                linger_ms=200,       # 稍微攒一下批，提升吞吐
            )
            print(f"[kafka] 已连接 {KAFKA_BOOTSTRAP}")
            return producer
        except NoBrokersAvailable:
            print(f"[kafka] 第 {attempt} 次连接失败，2 秒后重试…")
            time.sleep(2)
    raise RuntimeError(f"无法连接 Kafka（{KAFKA_BOOTSTRAP}），请确认容器已启动。")


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
        print(f"[opensky] 请求异常：{e}")
        return None

    # 429 = 额度用完，读一下要等多久
    if resp.status_code == 429:
        retry = resp.headers.get("X-Rate-Limit-Retry-After-Seconds", "?")
        print(f"[opensky] 额度用尽(429)，需等待 {retry} 秒")
        return None
    if resp.status_code == 400:
        print("[opensky] 400：通常是时间戳超过 1 小时限制，检查请求参数")
        return None
    if resp.status_code != 200:
        print(f"[opensky] 非预期状态码 {resp.status_code}")
        return None

    # 顺便打印剩余额度，方便你盯着别超
    remaining = resp.headers.get("X-Rate-Limit-Remaining")
    if remaining is not None:
        print(f"[opensky] 本次请求成功，剩余额度 {remaining}")

    return resp.json()


def run():
    print("=" * 56)
    print("  OpenSky → Kafka 采集器启动")
    print(f"  区域(bbox): {BBOX}")
    print(f"  采集间隔  : {POLL_INTERVAL} 秒")
    print(f"  目标 topic: {KAFKA_TOPIC} @ {KAFKA_BOOTSTRAP}")
    print("=" * 56)

    tokens = TokenManager(CLIENT_ID, CLIENT_SECRET)
    producer = create_kafka_producer()

    # 优雅退出：Ctrl+C 时把缓冲刷完再退
    def shutdown(signum, frame):
        print("\n[exit] 收到停止信号，刷新缓冲并关闭…")
        producer.flush()
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    round_no = 0
    while True:
        round_no += 1
        data = fetch_states(tokens)

        if data and data.get("states"):
            snapshot_time = data.get("time")
            states = data["states"]
            sent = 0
            for state in states:
                record = parse_state(state)
                # 附上本次快照时间，供下游做事件时间用
                record["snapshot_time"] = snapshot_time
                # 用 icao24 当 key，保证同一架飞机进同一分区、顺序一致
                producer.send(KAFKA_TOPIC, key=record["icao24"], value=record)
                sent += 1
            producer.flush()
            print(f"[round {round_no}] 已发送 {sent} 条航班记录 → Kafka")
        else:
            print(f"[round {round_no}] 本轮无数据（可能该区域此刻航班少或请求失败）")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
