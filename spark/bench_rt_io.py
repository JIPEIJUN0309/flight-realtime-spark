"""
M3 实时链路 I/O 诊断：微批为什么比 5 秒的触发间隔还慢
=====================================================
M1 实时运行时 ODS 每批 6–7 秒、dwd_state 每批 7–9 秒，而一批只有一张快照（约 900 行），
计算本身用不了这么久。这个脚本不连 Kafka、不花 OpenSky 额度，分几项测时间花在哪：

  1. 文件系统元数据操作：建、查、列、改名、删小文件。/data 是 Windows 的 D 盘挂进容器的，
     /tmp 在容器自己的 Linux 文件系统上，两边各测一遍
  2. 现有 Delta 表的日志：_delta_log 里有多少个文件，列一遍要多久。
     Delta 每次读写前都要列这个目录找最新版本，文件越多越慢
  3. 空 Spark 作业的固定开销
  4. 同样的微批（900 行，和一张快照差不多）写 Delta：追加、只插入的 MERGE（同 dwd_state）、
     按主键 upsert 的 MERGE（同最新状态），在 /data 和 /tmp 各提交若干次
  5. 航段回看：从状态明细读最近 20 分钟，涉及多少个文件、要多久（M2 的流式每批都要读一次）

跑法（spark 容器里，约 3–6 分钟，结果写到仓库根目录的日志）：
  docker exec spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/bench_rt_io.py > m3_diag.log 2>&1

测试表建在 /data/bench/rt_io 和 /tmp/rt_io，测完删掉；现有的表只读不写。
"""

import os
import shutil
import statistics
import time
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_ROOT = os.getenv("BENCH_DATA_ROOT", "/data/bench/rt_io")
LOCAL_ROOT = os.getenv("BENCH_LOCAL_ROOT", "/tmp/rt_io")
ROUNDS = int(os.getenv("BENCH_ROUNDS", "12"))   # 每个位置提交几轮；前 3 轮（建表 + 预热）不计
SKIP = 3
ROWS = 900                                      # 一张快照大约这么多架飞机
FS_FILES = 300

TABLES = [
    ("bronze_flights", "/data/delta/bronze_flights"),
    ("dwd_flight_state", "/data/delta/dwd_flight_state"),
    ("dwd_airport_event", "/data/delta/dwd_airport_event"),
    ("dwd_flight_leg", "/data/delta/dwd_flight_leg"),
    ("ads/latest_position", "/data/delta/ads/latest_position"),
]
CHECKPOINT_ROOT = "/data/checkpoint"
STATE_PATH = "/data/delta/dwd_flight_state"
LEG_PATH = "/data/delta/dwd_flight_leg"

TS_FMT = "yyyy-MM-dd HH:mm:ss"
T0 = int(datetime(2026, 10, 20, tzinfo=timezone.utc).timestamp())

INSERT_MERGE = """
MERGE INTO delta.`{path}` AS t
USING bench_batch AS s
ON  t.dt BETWEEN DATE '{d}' AND DATE '{d}'
AND t.event_time BETWEEN TIMESTAMP '{ts}' AND TIMESTAMP '{ts}'
AND t.icao24 = s.icao24
AND t.event_time = s.event_time
WHEN NOT MATCHED THEN INSERT *
"""

UPSERT_MERGE = """
MERGE INTO delta.`{path}` AS t
USING bench_batch AS s
ON t.icao24 = s.icao24
WHEN MATCHED AND s.event_time > t.event_time THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("M3-IO-Diagnosis")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def section(title: str) -> None:
    print("\n" + "=" * 64 + f"\n  {title}\n" + "=" * 64, flush=True)


def ms(seconds: float) -> str:
    return f"{seconds * 1000:.0f} ms"


def describe(samples: list) -> str:
    """中位数 / P90 / 最大。"""
    s = sorted(samples)
    p90 = s[min(len(s) - 1, int(round(0.9 * (len(s) - 1))))]
    return f"中位数 {ms(statistics.median(s))}，P90 {ms(p90)}，最大 {ms(s[-1])}（{len(s)} 次）"


def timed(fn) -> float:
    t = time.perf_counter()
    fn()
    return time.perf_counter() - t


def safe_root(root: str) -> str:
    # 下面会整个删掉这个目录，只允许是测试目录
    if not root.rstrip("/").endswith("rt_io"):
        raise ValueError(f"测试目录必须以 rt_io 结尾：{root}")
    return root


# ================================================================
#  0. 环境
# ================================================================
def show_env(spark: SparkSession) -> None:
    section("0. 环境")
    with open("/proc/mounts") as f:
        for line in f:
            dev, mnt, fstype = line.split()[:3]
            if mnt in ("/", "/data", "/tmp") or mnt.startswith("/data/"):
                print(f"  挂载 {mnt:<10} 文件系统 {fstype:<10} 来源 {dev}")
    print(f"  CPU 核数     : {os.cpu_count()}")
    for name in ("memory.max", "memory.current"):
        path = f"/sys/fs/cgroup/{name}"
        if os.path.exists(path):
            with open(path) as f:
                value = f.read().strip()
            shown = f"{int(value) / 2**20:.0f} MB" if value.isdigit() else value
            print(f"  容器内存 {name:<15}: {shown}")
    print(f"  driver 内存  : {spark.conf.get('spark.driver.memory', '1g（默认）')}")


# ================================================================
#  1. 文件系统元数据操作
# ================================================================
def fs_bench(label: str, root: str) -> dict:
    d = os.path.join(safe_root(root), "fs_ops")
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    names = [os.path.join(d, f"f{i:04d}") for i in range(FS_FILES)]

    def create():
        for n in names:
            with open(n, "w") as f:
                f.write("x")

    def stat():
        for n in names:
            os.stat(n)

    def list_dir():
        # 列目录并取每个文件的属性：Hadoop 的 listStatus 就是这么做的
        for e in os.scandir(d):
            e.stat()

    def rename():
        # Delta 写提交日志是"先写临时文件、再改名"
        for n in names:
            os.replace(n, n + ".r")

    def delete():
        for n in names:
            os.remove(n + ".r")

    result = {}
    for op, fn in (("建", create), ("查属性", stat), ("列目录", list_dir), ("改名", rename), ("删", delete)):
        result[op] = timed(fn) / FS_FILES
    shutil.rmtree(d, ignore_errors=True)
    print(f"  {label:<24}" + "，".join(f"{op} {sec * 1000:.3f} ms/个" for op, sec in result.items()))
    return result


# ================================================================
#  2. 现有 Delta 表的日志目录
# ================================================================
def existing_logs(spark: SparkSession) -> None:
    section("2. 现有 Delta 表：日志文件数、列一次日志目录、加载一次表")
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    for name, path in TABLES:
        log_dir = os.path.join(path, "_delta_log")
        if not os.path.isdir(log_dir):
            print(f"  {name:<20} 不存在，跳过")
            continue
        entries = os.listdir(log_dir)
        n_json = sum(1 for e in entries if e.endswith(".json"))
        hpath = jvm.org.apache.hadoop.fs.Path("file://" + log_dir)
        fs = hpath.getFileSystem(conf)
        listing = [timed(lambda: fs.listStatus(hpath)) for _ in range(2)]
        # 加载表时 Delta 会刷新快照：列日志目录，读最新的 checkpoint 和之后的提交
        loads = [timed(lambda: spark.read.format("delta").load(path).schema) for _ in range(3)]
        n_files = len(spark.read.format("delta").load(path).inputFiles())
        print(f"  {name:<20} 日志目录 {len(entries):>6} 个文件（提交 {n_json} 次）；"
              f"Hadoop 列一次 {ms(min(listing))}–{ms(max(listing))}；"
              f"加载表 首次 {ms(loads[0])}、之后 {ms(statistics.median(loads[1:]))}；"
              f"数据文件 {n_files} 个")
    if os.path.isdir(CHECKPOINT_ROOT):
        for q in sorted(os.listdir(CHECKPOINT_ROOT)):
            total = sum(len(files) for _, _, files in os.walk(os.path.join(CHECKPOINT_ROOT, q)))
            print(f"  checkpoint/{q:<26} {total} 个文件")


# ================================================================
#  3. 空作业
# ================================================================
def job_overhead(spark: SparkSession) -> None:
    section("3. 空 Spark 作业的固定开销")
    spark.range(1000).count()
    samples = [timed(lambda: spark.range(1000).count()) for _ in range(15)]
    print(f"  spark.range(1000).count()：{describe(samples)}")


# ================================================================
#  4. 同样的微批写 Delta
# ================================================================
def make_batch(spark: SparkSession, k: int):
    """合成一张快照：900 架飞机、同一个快照时间，列和类型接近状态明细。"""
    t = T0 + 10 * k
    return (
        spark.range(ROWS)
        .select(
            F.format_string("b%05x", "id").alias("icao24"),
            F.timestamp_seconds(F.lit(t)).alias("event_time"),
            F.concat(F.lit("TST"), F.col("id").cast("string")).alias("callsign"),
            (F.lit(43.0) + F.rand(k) * 9).alias("latitude"),
            (F.lit(2.5) + F.rand(k + 1000) * 10).alias("longitude"),
            F.round(F.rand(k + 2000) * 12000, 0).alias("altitude_m"),
            F.round(F.rand(k + 3000) * 900, 1).alias("speed_kmh"),
            (F.rand(k + 4000) > 0.9).alias("on_ground"),
            F.timestamp_seconds(F.lit(t + 1)).alias("kafka_ts"),
        )
        .withColumn("dt", F.to_date("event_time"))
    )


def delta_bench(spark: SparkSession, label: str, root: str) -> dict:
    root = safe_root(root)
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    paths = {name: os.path.join(root, name) for name in ("append", "state", "latest")}
    times = {"追加": [], "只插入 MERGE": [], "upsert MERGE": []}
    for k in range(ROUNDS):
        df = make_batch(spark, k).persist()
        df.count()
        df.createOrReplaceTempView("bench_batch")
        ts = datetime.fromtimestamp(T0 + 10 * k, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        times["追加"].append(timed(
            lambda: df.write.format("delta").mode("append").save(paths["append"])))
        if k == 0:
            df.write.format("delta").partitionBy("dt").save(paths["state"])
            df.write.format("delta").save(paths["latest"])
            times["只插入 MERGE"].append(0.0)
            times["upsert MERGE"].append(0.0)
        else:
            times["只插入 MERGE"].append(timed(lambda: spark.sql(INSERT_MERGE.format(
                path=paths["state"], d=ts[:10], ts=ts)).collect()))
            times["upsert MERGE"].append(timed(lambda: spark.sql(UPSERT_MERGE.format(
                path=paths["latest"])).collect()))
        df.unpersist()
    n_log = len(os.listdir(os.path.join(paths["append"], "_delta_log")))
    print(f"  {label}（{ROUNDS} 轮，前 {SKIP} 轮建表和预热不计；追加表的日志目录最后有 {n_log} 个文件）")
    for op, samples in times.items():
        print(f"    {op:<14}{describe(samples[SKIP:])}")
    shutil.rmtree(root, ignore_errors=True)
    return {op: statistics.median(samples[SKIP:]) for op, samples in times.items()}


# ================================================================
#  5. 航段回看
# ================================================================
def lookback_bench(spark: SparkSession) -> None:
    section("5. 航段回看：读状态明细最近 20 分钟（M2 流式每批读一次）")
    states = spark.read.format("delta").load(STATE_PATH)
    latest = states.agg(F.date_format(F.max("event_time"), TS_FMT)).first()[0]
    if latest is None:
        print("  状态明细是空的，跳过")
        return
    start = (datetime.strptime(latest, "%Y-%m-%d %H:%M:%S") - timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")
    window = (states
              .where(f"dt BETWEEN DATE '{start[:10]}' AND DATE '{latest[:10]}'")
              .where(f"event_time >= TIMESTAMP '{start}' AND event_time <= TIMESTAMP '{latest}'"))
    n_files = window.select(F.input_file_name()).distinct().count()
    samples = [timed(lambda: window.count()) for _ in range(3)]
    print(f"  区间 [{start}, {latest}] UTC：{window.count()} 行，落在 {n_files} 个文件里；"
          f"读一次 首次 {ms(samples[0])}、之后 {ms(statistics.median(samples[1:]))}")
    if os.path.isdir(os.path.join(LEG_PATH, "_delta_log")):
        legs = spark.read.format("delta").load(LEG_PATH).where(f"last_seen >= TIMESTAMP '{start}'")
        n_leg_files = legs.select(F.input_file_name()).distinct().count()
        leg_samples = [timed(lambda: legs.count()) for _ in range(3)]
        print(f"  航段表 last_seen ≥ {start}：{legs.count()} 行，{n_leg_files} 个文件；"
              f"读一次 首次 {ms(leg_samples[0])}、之后 {ms(statistics.median(leg_samples[1:]))}")


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    started = time.time()

    show_env(spark)

    section("1. 文件系统元数据操作（每个操作的平均耗时）")
    on_data = fs_bench("/data（Windows 挂载）", DATA_ROOT)
    on_local = fs_bench("/tmp（容器内）", LOCAL_ROOT)
    print("  /data 是 /tmp 的倍数：" + "，".join(
        f"{op} {on_data[op] / max(on_local[op], 1e-9):.0f} 倍" for op in on_data))

    existing_logs(spark)
    job_overhead(spark)

    section(f"4. 同样的微批（{ROWS} 行）写 Delta，每次提交的耗时")
    data = delta_bench(spark, "/data（Windows 挂载）", DATA_ROOT)
    local = delta_bench(spark, "/tmp（容器内）", LOCAL_ROOT)
    print("  中位数对比：" + "，".join(
        f"{op} {ms(data[op])} vs {ms(local[op])}" for op in data))

    lookback_bench(spark)

    section(f"完成，共 {time.time() - started:.0f} 秒")
    spark.stop()


if __name__ == "__main__":
    main()
