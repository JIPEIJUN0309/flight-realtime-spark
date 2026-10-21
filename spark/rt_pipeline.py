"""
实时链路：一个流作业，一个微批写完 ODS、状态明细、起降事件、航段、最新状态和服务层
==========================================================================
读 Kafka，每 5 秒一批。M3 起取代原来常驻的 ods_bronze.py、dwd_state.py、dwd_event_leg.py 三个流作业：

  1. 解析 Kafka 消息（ods_bronze.to_bronze）→ 清洗去重（flight_rules，和 dwd_state 同一份规则）；
     bronze 同时在后台读这一批、按 offset 只插入
  2. 判起降、切航段（flight_legs.stream_step）；最新状态要改的行（latest_position.latest_updates）在后台算
  3. 同时写服务层和湖：一个 PostgreSQL 事务写最新状态、航段、起降事件（pg_sync，处理延迟算到它提交）；
     湖里起降事件、航段、最新状态并发提交；等 bronze 写完，状态明细最后提交

跑法（spark 容器里；时间按 UTC 解释，区间左闭右开）：
  spark-submit --driver-memory 2g jobs/rt_pipeline.py                          # stream，常驻
  spark-submit --driver-memory 2g jobs/rt_pipeline.py stream --available-now   # 处理完 Kafka 里的积压就退出
  spark-submit jobs/rt_pipeline.py verify --start "2026-10-20 08:00" --end "2026-10-20 10:00"
      状态明细、起降事件和航段按区间对账；最新状态全表对账；服务层和湖逐行比。先停掉 stream 再跑
  停止：在作业窗口按 Ctrl+C（用 docker exec -it 启动时），或者 docker exec spark pkill -f rt_pipeline.py。
      两种都会等正在写的这一批写完再停；急着停就再按一次 Ctrl+C，没写完的那一批重启后整批重放

★ 为什么合成一个作业
  原来三个作业首尾相接：每一级都要等自己的触发，再花一批的处理时间；三个作业的触发又都对齐在
  5 秒整点上，上游提交后要等到下一个整点才被下游读到。合成一个作业，只读一次 Kafka、只等一次触发，
  处理延迟 = 等触发（最多 5 秒）+ 算完写进 PostgreSQL 的时间。三个 JVM 也变成一个。

★ 服务层和湖为什么同时写
  处理延迟按"写进服务层的时间"算，服务层不用等 Delta 表提交完；两边互不依赖，同时写，整批也更快结束。
  两边都按主键幂等：这一批任何一步失败，作业停下，重启后整批从 checkpoint 重放，PostgreSQL 再 upsert
  一遍同样的行，Delta 只插入或按主键替换，不会写重。湖是事实来源：作业每次启动先把服务层按湖全量对齐一次。

★ 每张表的幂等键
  bronze_flights       Kafka (partition, offset)，只插入。重放、删了 checkpoint 从头读都不会重复
  dwd_flight_state     (icao24, event_time)，只插入（和 dwd_state 的 MERGE 结果相同）
  dwd_airport_event    (icao24, event_time, event_type)，只插入
  dwd_flight_leg       leg_id，整行替换（合并规则单调，见 flight_legs；写法见 replace_rows）
  ads_latest_position  icao24，整行替换（只取更晚的记录，见 latest_position）

★ 只插入为什么不用 MERGE
  Delta 的只插入 MERGE 在内部就是"在相关范围内和表做反连接，再追加"。这里直接这么写：
  按时间或 offset 范围读出表里已有的主键（Delta 用文件的 min/max 统计跳过无关文件），反连接后追加。
  M3 诊断在容器内实测：同样 900 行，只插入 MERGE 每次 0.74 秒，追加 0.16 秒。
  追加不像 MERGE 那样检查并发冲突，所以再加两道保险（M3 冒烟测试时误开了两个实时作业，暴露了这个口子）：
    1. 启动时检查容器里有没有另一个实时作业在跑，有就退出
    2. 追加带 Delta 的事务标识（txnAppId = 流式查询 id + 表，txnVersion = 批号）：同一批已经提交过，
       再提交一次 Delta 会直接跳过。查询 id 跟着 checkpoint 走，删了 checkpoint 重来是新的 id，不会误跳过

★ 为什么状态明细最后提交
  判起降、切航段只看本批里状态明细还没有的行（采集端重复拉到的同一张快照不再算一遍）。状态明细是
  这一批最后一个提交的：它提交了，说明其余四张表都已写完；它没提交就失败了，重放时这些行还"没有"，
  会被完整地重算一遍。前四张表的写入都幂等，重算写进去的是同一组行。
  本批最早的快照晚于数据时钟（状态明细里最晚的快照）时，这些行不可能已经在表里，不读表做反连接；
  数据时钟启动时从状态明细取，之后每批写完状态明细才往后推，所以这个判断和读表的结果一样。

★ 每批的时间花在哪、怎么省（M3 第一次冒烟测试每批 6–9 秒，目标 5 秒以内）
  一批只有几百行，时间几乎都是固定开销：十几个小 Spark 作业各自排计划、起任务，Delta 每读到一张表的新版本
  就重建一次快照。所以省时间靠少做几次、并行做，而不是算得更快：
    1. Delta 重建快照只用 1 个任务（build_spark 里的 snapshotPartitions，默认 50）
    2. 存中间结果的同一个作业里顺便算出后面要用的行数和最小最大值（Cache.stats），不再单独跑聚合
    3. 并行：bronze 在后台读 Kafka、写表；最新状态在后台算；判起降时"上一个航段"和"翻转"同时算；
       服务层和湖同时写
    4. 正常运行时本批都是新快照，不读状态明细做去重的反连接（见上一节），起降事件也直接追加
    5. 航段、最新状态按主键整段替换（replace_rows），一个作业写完，不用 MERGE 的两三个作业
    6. 每次追加合成一个文件（append_rows），跑几个小时表里也不会堆出几千个小文件
  后台线程要先设当前会话（in_session），否则解析 SQL 文本时用的不是这个会话的时区。

★ 平稳停止
  Ctrl+C 或 pkill 时 JVM 不理信号，由 Python 等正在写的这一批写完、停掉查询再退出（StopRequest、run_until_stopped）。

★ ingest_time 重放时不变
  to_bronze 里的 current_timestamp() 写在流式查询的计划里，Spark 每批把它换成这一批的触发时间并记进
  checkpoint。bronze 和状态明细各读一次这一批的 Kafka 消息，入湖时间相同；重放时还是那个值，回刷对账对得上。
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import StructField, StructType

import dwd_event_leg
import dwd_state
import pg_sync
from flight_legs import EVENT_KEY, LEG_KEY, STATE_COLUMNS, TS_FMT, batch_bounds, stream_step
from flight_rules import KEY_COLUMNS, airports_column, clean_states, load_airports
from latest_position import LATEST_COLUMNS, LATEST_KEY, latest_updates
from ods_bronze import to_bronze

# ---------------- 路径与参数（环境变量可覆盖）----------------
KAFKA_BOOTSTRAP = os.getenv("RT_KAFKA_BOOTSTRAP", "kafka:29092")   # 容器之间用服务名，不是宿主机的 localhost:9092
KAFKA_TOPIC = os.getenv("RT_KAFKA_TOPIC", "flights")
BRONZE_PATH = dwd_state.BRONZE_PATH
DWD_STATE_PATH = dwd_state.DWD_STATE_PATH
EVENT_PATH = dwd_event_leg.EVENT_PATH
LEG_PATH = dwd_event_leg.LEG_PATH
LATEST_PATH = os.getenv("ADS_LATEST_PATH", "/data/delta/ads/latest_position")
CHECKPOINT_PATH = os.getenv("RT_CHECKPOINT", "/data/checkpoint/rt_pipeline")
TRIGGER_INTERVAL = os.getenv("RT_TRIGGER_INTERVAL", "5 seconds")
# 停机后积压很多时，每批最多读这么多条 Kafka 消息（10 秒一轮、每轮约 900 条时约 18 分钟的数据）
MAX_OFFSETS_PER_TRIGGER = os.getenv("RT_MAX_OFFSETS_PER_TRIGGER", "100000")
WRITE_ATTEMPTS = 3
EPOCH = "1970-01-01 00:00:00"
# 每批都要提交、又在关键路径上的四张表，checkpoint 间隔从默认 10 次提交改成 50 次，见 ensure_checkpoint_interval
CHECKPOINT_INTERVAL = os.getenv("RT_DELTA_CHECKPOINT_INTERVAL", "50")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RT-Pipeline")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        # Delta 每次读到表的新版本，都要从 checkpoint 和之后的提交日志重建一遍"表里有哪些文件"，默认拆成
        # 50 个任务。这几张表每批都有新提交，一批要重建五张表；文件只有几百上千个，一个任务就够了。
        # 云端本地压测：只改这一项，每批从 48 秒降到 25 秒（2 核机器，任务多的代价被放大）
        .config("spark.databricks.delta.snapshotPartitions", "1")
        # dt 按 UTC 日期分区，命令行给的时间也按 UTC 解释
        .config("spark.sql.session.timeZone", "UTC")
        # 常驻作业每批跑二十来个查询：Spark UI 只留最近的，计划字符串再加个上限，driver 内存不会越用越多
        .config("spark.sql.ui.retainedExecutions", "100")
        .config("spark.ui.retainedJobs", "200")
        .config("spark.ui.retainedStages", "200")
        .config("spark.sql.maxPlanStringLength", "200000")
        # 不再用的缓存块由 driver 的垃圾回收触发清理，默认 30 分钟一次，这里改成 2 分钟
        .config("spark.cleaner.periodicGC.interval", "2min")
        .getOrCreate()
    )


def with_load_time(df: DataFrame, name: str = "load_time") -> DataFrame:
    """写入湖的时间。它随写入时刻变化，对账时不比这一列。"""
    return df.withColumn(name, F.current_timestamp())


def utc_epoch(text: str) -> float:
    fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in text else "%Y-%m-%d %H:%M:%S"
    return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc).timestamp()


# ================================================================
#  启动：检查湖里的表，建最新状态表，服务层全量对齐
# ================================================================
def ensure_bronze(spark: SparkSession) -> None:
    """bronze 不存在就按 to_bronze 的输出建（和 ods_bronze.py 写出来的表结构一样）。"""
    if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        return
    kafka_like = spark.createDataFrame(
        [], "key binary, value binary, topic string, partition int, offset long, timestamp timestamp")
    (DeltaTable.createIfNotExists(spark).location(BRONZE_PATH)
     .addColumns(to_bronze(kafka_like).schema).execute())


def latest_schema(spark: SparkSession) -> StructType:
    """最新状态表按代码应有的结构：latest_updates 的输出加 load_time，主键 NOT NULL。"""
    states = spark.read.format("delta").load(DWD_STATE_PATH).limit(0)
    current = states.withColumn("departed", F.lit(False)).select(*LATEST_COLUMNS)
    df = with_load_time(latest_updates(current, states, EPOCH))
    return StructType([StructField(f.name, f.dataType, nullable=f.name not in LATEST_KEY)
                       for f in df.schema.fields])


def ensure_latest(spark: SparkSession, clock: str) -> None:
    """最新状态表不存在就建，并从状态明细全量算一遍；结构和代码不一致就退出，提示删表。"""
    expected = latest_schema(spark)
    if DeltaTable.isDeltaTable(spark, LATEST_PATH):
        diff = dwd_event_leg.schema_diff(spark.read.format("delta").load(LATEST_PATH).schema, expected)
        if diff:
            sys.exit(f"[rt] {LATEST_PATH} 的表结构和代码不一致（{diff}）。可能是批链路以前写的旧表，"
                     "删掉后重启本作业，会从状态明细重建：\n"
                     f"  docker exec spark rm -rf {LATEST_PATH}")
        return
    started = time.time()
    (DeltaTable.createIfNotExists(spark).location(LATEST_PATH).addColumns(expected).execute())
    states = spark.read.format("delta").load(DWD_STATE_PATH)
    current = spark.read.format("delta").load(LATEST_PATH).select(*LATEST_COLUMNS)
    rows = with_load_time(latest_updates(current, states, clock))
    rows.write.format("delta").mode("append").save(LATEST_PATH)
    n = spark.read.format("delta").load(LATEST_PATH).count()
    print(f"[rt] 最新状态表不存在，已从状态明细全量建表：{n} 架飞机，耗时 {time.time() - started:.1f} 秒")


def ensure_checkpoint_interval(spark: SparkSession) -> None:
    """状态明细、起降事件、航段、最新状态四张表的 checkpoint 间隔设成 CHECKPOINT_INTERVAL 次提交（默认 10）。

    Delta 每提交 10 次就在那次提交里同步写一个 checkpoint（全表文件清单写成 parquet），那一批的这张表要多
    0.6–0.9 秒（第三次冒烟测试实测：状态明细 0.2 → 1.1 秒，航段 0.7 → 1.4 秒）。实时作业每批每张表提交一次，
    默认间隔下每 10 批就有一两批因此接近 5 秒。改成 50 次，这种批少到五分之一；代价是读表重建快照时要多读
    几十个很小的提交日志文件。表属性改动是一次提交，只在启动时、属性不一致时改。bronze 在后台写，不在关键路径上，不改。
    """
    for path in (DWD_STATE_PATH, EVENT_PATH, LEG_PATH, LATEST_PATH):
        props = DeltaTable.forPath(spark, path).detail().select("properties").first()[0] or {}
        if props.get("delta.checkpointInterval") != CHECKPOINT_INTERVAL:
            spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES "
                      f"('delta.checkpointInterval' = '{CHECKPOINT_INTERVAL}')")
            print(f"[rt] {path} 的 checkpoint 间隔设为每 {CHECKPOINT_INTERVAL} 次提交一次")


def data_clock(spark: SparkSession) -> str:
    """数据时钟：状态明细里最晚的快照时间（UTC 字符串）。"""
    v = spark.read.format("delta").load(DWD_STATE_PATH).agg(F.date_format(F.max("event_time"), TS_FMT)).first()[0]
    return v or EPOCH


def lake_frames(spark: SparkSession) -> dict:
    return {
        "latest": spark.read.format("delta").load(LATEST_PATH),
        "legs": spark.read.format("delta").load(LEG_PATH),
        "events": spark.read.format("delta").load(EVENT_PATH),
    }


def starting_offsets(spark: SparkSession) -> str:
    """第一次启动（还没有 checkpoint）时从哪读 Kafka：bronze 里每个分区最大的 offset 之后。

    从原来的 ods_bronze.py 切过来时不重读、不漏读。有 checkpoint 时 Spark 从 checkpoint 续读，这个值不生效。
    bronze 是空的就从最早的读。
    """
    if os.path.isdir(os.path.join(CHECKPOINT_PATH, "offsets")):
        return "earliest"
    rows = (spark.read.format("delta").load(BRONZE_PATH)
            .where(F.col("topic") == KAFKA_TOPIC)
            .groupBy("partition").agg(F.max("offset").alias("max_offset")).collect())
    if not rows:
        return "earliest"
    return json.dumps({KAFKA_TOPIC: {str(r["partition"]): int(r["max_offset"]) + 1 for r in rows}})


# ================================================================
#  一个微批
# ================================================================
class Cache:
    """把中间结果物化成执行计划里的叶子节点（localCheckpoint），记下行数；批结束时统一释放。

    也作为 flight_legs.stream_step 的 cache 参数。

    ★ 为什么不用 persist（dwd_event_leg.Materializer 的做法）
      persist 之后的 DataFrame，执行计划里还挂着它的整棵来源树：InMemoryRelation 打印时会把缓存时的计划
      一起打印。航段一步层层引用前面缓存的结果，同一棵子树被打印很多遍。本地模拟实测第一批的航段行，
      计划字符串就有 71 MB；AQE 每次改计划、Spark UI 每记一次查询都要生成它，常驻作业几十批就会把
      driver 内存撑爆。localCheckpoint 把结果存进 executor 的内存（放不下落盘）并切断血缘，之后的计划里
      它只是一个叶子节点，计划小、优化也快。
      先标记、再 count：一个作业里既算出结果又存好，不用为物化单独多跑一次。
    """

    def __init__(self):
        self.cached = []
        self.sizes = {}

    def __call__(self, df: DataFrame) -> DataFrame:
        df = df.localCheckpoint(eager=False)
        self.sizes[id(df)] = df.count()
        self.cached.append(df)
        return df

    def stats(self, df: DataFrame, *aggs):
        """和 __call__ 一样存下 df，同一个作业里顺便算几个聚合值，返回 (df, 聚合结果行)。

        count 本身就是一次聚合；把后面要用的最小最大值放进同一次聚合，少跑一个 Spark 作业。
        结果行里 _n 是行数。
        """
        df = df.localCheckpoint(eager=False)
        row = df.agg(F.count(F.lit(1)).alias("_n"), *aggs).first()
        self.sizes[id(df)] = row["_n"]
        self.cached.append(df)
        return df, row

    def size(self, df: DataFrame) -> int:
        return self.sizes[id(df)]

    def release(self) -> None:
        """释放本批存下的数据块。不释放的话要等 driver 做垃圾回收才清，常驻作业会越积越多。"""
        for df in reversed(self.cached):
            try:
                df._jdf.queryExecution().analyzed().rdd().unpersist(False)
            except Exception:  # noqa: BLE001 —— 释放失败只是晚点回收，不影响结果
                pass
        self.cached.clear()
        self.sizes.clear()


def with_retry(fn, batch_id: int, name: str) -> float:
    """执行一次写入，遇到 Delta 并发冲突（例如有人同时在回刷）就重试。写入都按主键幂等，重试安全。返回耗时。"""
    started = time.perf_counter()
    for attempt in range(1, WRITE_ATTEMPTS + 1):
        try:
            fn()
            return time.perf_counter() - started
        except dwd_state.DELTA_CONFLICTS as e:
            if attempt == WRITE_ATTEMPTS:
                raise
            print(f"[rt] 批 {batch_id}：{name} 遇到并发冲突（{type(e).__name__}），{attempt} 秒后重试")
            time.sleep(attempt)
    return time.perf_counter() - started


def timed(fn) -> float:
    """执行 fn，返回耗时（秒）。"""
    started = time.perf_counter()
    fn()
    return time.perf_counter() - started


def in_session(spark: SparkSession, fn, *args):
    """在线程池里调用 fn 之前，先把这个线程在 JVM 一侧的"当前会话"设成 spark。

    PySpark 每个 Python 线程对应一个 JVM 线程，新线程里没有当前会话。这时解析 SQL 文本（where 字符串、
    F.expr）用的是默认配置而不是会话配置：会话时区退回 JVM 的默认时区，TIMESTAMP '...' 常量会差出
    时区那么多小时。云端本地压测（机器时区是 UTC+8）时，后台线程算的"已离开"全错了，就是这个原因。
    """
    spark._jvm.org.apache.spark.sql.SparkSession.setActiveSession(spark._jsparkSession)
    return fn(*args)


def append_new(spark: SparkSession, df: DataFrame, path: str, keys, where: str, txn=None) -> float:
    """只插入表里没有的主键：在 where 限定的范围内和表做反连接，再追加。返回耗时。

    where 是作用在表上的常量条件（offset 或时间范围），Delta 据此用文件统计跳过无关文件，
    正常情况下一个文件都不用读。txn = (txnAppId, txnVersion)：同一个标识和批号只会提交一次。
    """
    started = time.perf_counter()
    existing = spark.read.format("delta").load(path).where(where).select(*keys)
    append_rows(df.join(existing, list(keys), "left_anti"), path, txn)
    return time.perf_counter() - started


def replace_rows(spark: SparkSession, rows: DataFrame, path: str, keys, where, partition_col, txn) -> None:
    """按主键整行替换：表里（where 范围内）不在 rows 里的行，加上 rows，一次覆盖写回。返回耗时。

    结果和 MERGE ... WHEN MATCHED UPDATE SET * WHEN NOT MATCHED INSERT * 相同（云端压测 8 批逐行比过），
    但只跑一个作业：MERGE 先连接一次找出要改的文件，再连接一次重写，一批要两三个作业。
    范围内只有几千行（最新状态整张表、航段从本批最早的开始日期起），整段重写更快。
    where 为空时覆盖整张表；不为空时用 replaceWhere，只替换这个范围，范围外的分区不动。
    带事务标识：同一批重放时，已经提交过的这一步直接跳过。
    """
    current = spark.read.format("delta").load(path)
    if where:
        current = current.where(where)
    full = current.join(rows.select(*keys), list(keys), "left_anti").unionByName(rows)
    full = full.repartition(partition_col) if partition_col else full.coalesce(1)
    writer = full.write.format("delta").mode("overwrite")
    if where:
        writer = writer.option("replaceWhere", where)
    writer.option("txnAppId", txn[0]).option("txnVersion", int(txn[1])).save(path)


def append_rows(df: DataFrame, path: str, txn=None) -> None:
    # 每批只有几百行：合成一个文件再写（按日期分区的表每个分区一个文件）。不合的话反连接后的
    # 分区数（spark.sql.shuffle.partitions）就是文件数，跑两小时每张表会多出几千个小文件，
    # Delta 每次重建快照、按统计跳文件都要过一遍它们，越跑越慢
    writer = df.coalesce(1).write.format("delta").mode("append")
    if txn is not None:
        writer = writer.option("txnAppId", txn[0]).option("txnVersion", int(txn[1]))
    writer.save(path)


def other_streams_running() -> list:
    """容器里另外还在跑的实时作业（verify 不算），返回它们的 PID。

    两个实时作业用同一个 checkpoint，Spark 要到两边提交同一批时才报 CONCURRENT_STREAM_LOG_UPDATE，
    那时两边都已经写过湖了，所以启动前先查进程。
    """
    found = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit() or int(pid) == os.getpid():
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                args = [a.decode("utf-8", "replace") for a in f.read().split(b"\0") if a]
        except OSError:
            continue
        if (args and "python" in os.path.basename(args[0])
                and any(a.endswith("rt_pipeline.py") for a in args) and "verify" not in args):
            found.append(int(pid))
    return found


class StopRequest:
    """Ctrl+C（SIGINT）和 pkill（SIGTERM）的处理：第一次只记下来，由主线程等当前批写完再停；第二次立即退出。

    信号处理函数里只改标记、不打印也不调 Spark：信号可能正好打断主线程里的 py4j 调用，处理函数再发起
    py4j 调用就是重入（冒烟测试里 PySpark 自带的处理函数调 cancelAllJobs，打出了一串 Py4JError）。
    """

    def __init__(self):
        self.count = 0
        self.name = ""

    def __call__(self, signum, frame) -> None:
        self.count += 1
        self.name = signal.Signals(signum).name
        if self.count >= 2:
            # 直接写文件描述符，不经过 Python 的输出缓冲（缓冲区可能正被主线程占着）
            os.write(2, "\n[rt] 再次收到停止信号，立即退出。没写完的那一批，重启后会从 checkpoint 整批重放。\n"
                        .encode("utf-8"))
            os._exit(130)

    @property
    def requested(self) -> bool:
        return self.count > 0

    def install(self, spark: SparkSession) -> None:
        """JVM 忽略 INT 和 TERM，Python 这边接管。

        docker exec -it 里按 Ctrl+C，信号发给前台进程组里的每个进程：spark-submit 的 JVM 和它启动的 Python。
        JVM 默认收到就关机，正在写的那一批被打断；所以让 JVM 不理这两个信号，由 Python 等这一批写完、
        停掉查询、正常退出。Python 退出后 JVM 会跟着退出。
        """
        jvm = spark.sparkContext._jvm
        ignore = jvm.java.lang.Class.forName("sun.misc.SignalHandler").getField("SIG_IGN").get(None)
        for name in ("INT", "TERM"):
            jvm.sun.misc.Signal.handle(jvm.sun.misc.Signal(name), ignore)
        signal.signal(signal.SIGINT, self)
        signal.signal(signal.SIGTERM, self)


def run_until_stopped(query, batch: "RealtimeBatch", stop: StopRequest) -> None:
    """主线程等查询结束；收到停止请求后，等没有批在跑、最后一批的提交也记下了，再停查询。

    停在两批之间，Delta 和 checkpoint 都不会写到一半。就算赶上下一批刚开始（数据一直在来、批又跑得比
    触发间隔慢时可能发生），被打断的那一批重启后整批重放，各表的写入都幂等，结果不变。
    """
    announced = False
    while not query.awaitTermination(0.5):   # 返回 True：查询自己结束了（--available-now 处理完积压）
        if not stop.requested:
            continue
        if not announced:
            print(f"\n[rt] 收到 {stop.name}：等正在写的这一批写完再停，不会丢数据。"
                  "急着停就再按一次 Ctrl+C，没写完的那一批重启后会重放。", flush=True)
            announced = True
        # 等当前批写完：批结束时 batch.done 被置位，这里马上醒来，不靠轮询碰运气。
        # 数据一直在来、批又比触发间隔慢时，两批之间几乎没有空档，轮询很难正好赶上
        while batch.busy:
            batch.done.wait(0.2)
        # 批写完后 Spark 还要记 commit、更新进度，再停就不会打断它
        deadline = time.time() + 2
        while batch.last_done is not None and time.time() < deadline:
            progress = query.lastProgress
            if progress is not None and progress["batchId"] >= batch.last_done:
                break
            time.sleep(0.02)
        if batch.busy:
            continue   # 下一批已经开始了，等它也写完
        query.stop()
        print("[rt] 已停止。下次启动从 checkpoint 接着读 Kafka。", flush=True)
        return


def checkpoint_query_id() -> str:
    """流式查询的 id：第一次启动时写进 checkpoint 的 metadata，之后重启不变。"""
    with open(os.path.join(CHECKPOINT_PATH, "metadata"), encoding="utf-8") as f:
        return json.load(f)["id"]


class RealtimeBatch:
    """foreachBatch 的处理函数。数据时钟和 PostgreSQL 连接在批与批之间保留。"""

    def __init__(self, airports, specs: dict, clock: str, pg: pg_sync.PgClient):
        self.airports = airports
        self.specs = specs
        self.clock = clock
        self.pg = pg
        self.phases = {}       # batch_id → 各段耗时，监听器在这一批结束后取走写进 rt_batch_log
        self.query_id = None   # 第一批时从 checkpoint 读，作为追加的事务标识
        self.busy = False                # 正在处理一批；停止时主线程等它变回 False
        self.done = threading.Event()    # 每批结束时置位，停止时主线程靠它立刻知道这一批写完了
        self.last_done = None            # 最近一次处理完的批号

    def txn(self, table: str, batch_id: int):
        if self.query_id is None:
            self.query_id = checkpoint_query_id()
        return (f"rt_pipeline:{self.query_id}:{table}", batch_id)

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        started = time.perf_counter()
        self.done.clear()
        self.busy = True
        spark = batch_df.sparkSession
        cache = Cache()
        # 后台三件事：写 bronze、算最新状态、判起降时和"翻转"同时算"上一个航段"。它们互不依赖，并行做
        side = ThreadPoolExecutor(max_workers=3)
        try:
            # ---- 1. bronze：后台从 Kafka 读这一批（读一次存下来），按 offset 只插入；状态明细提交之前等它写完 ----
            def write_bronze():
                raw, o = cache.stats(batch_df, F.min("offset").alias("o_min"), F.max("offset").alias("o_max"))
                took = append_new(spark, raw, BRONZE_PATH, ("partition", "offset"),
                                  f"`offset` BETWEEN {o['o_min']} AND {o['o_max']}", self.txn("bronze", batch_id))
                return o["_n"], took

            bronze_job = side.submit(in_session, spark, write_bronze)

            # ---- 2. 状态明细：主线程同时也从 Kafka 读这一批、清洗，同一个作业里算出后面要用的范围 ----
            # 流式查询里 ingest_time 每批是一个固定值，两边各读一次 Kafka，写进 bronze 和状态明细的入湖时间相同
            cleaned, b = cache.stats(
                with_load_time(clean_states(batch_df, self.airports), "dwd_load_time"),
                *batch_bounds(),
                F.date_format(F.min("kafka_ts"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("k_min"),
                F.date_format(F.timestamp_micros(F.percentile_approx(F.unix_micros("kafka_ts"), 0.5)),
                              "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("k_p50"),
            )
            n_clean = b["_n"]
            if n_clean == 0:
                # 全被过滤掉：只落 ODS，其余不动
                n_raw, _ = bronze_job.result()
                print(f"[rt] 批 {batch_id}：读入 {n_raw} 条，清洗后为空，只写了 bronze", flush=True)
                self.last_done = batch_id
                return
            clock = max(self.clock, b["t_max"])
            t_clean = time.perf_counter()

            def compute_latest():
                t = time.perf_counter()
                df = cache(with_load_time(latest_updates(spark.read.format("delta").load(LATEST_PATH), cleaned, clock)))
                return df, time.perf_counter() - t

            # 最新状态只看本批状态明细和最新状态表，后台算
            latest_job = side.submit(in_session, spark, compute_latest)

            # ---- 3. 起降、航段 ----
            states = spark.read.format("delta").load(DWD_STATE_PATH)
            in_batch = (f"dt BETWEEN DATE '{b['t_min'][:10]}' AND DATE '{b['t_max'][:10]}' AND "
                        f"event_time BETWEEN TIMESTAMP '{b['t_min']}' AND TIMESTAMP '{b['t_max']}'")
            if b["t_min"] > self.clock:
                # 本批最早的快照也晚于状态明细里最晚的快照（数据时钟），这些行不可能已经在表里，
                # 不用读表做反连接。正常运行时每批都是新快照，都走这里；重放、采集端重复拉到同一张
                # 快照时，本批的时间不晚于数据时钟，走下面的反连接
                new_rows, nb = cleaned, b
            else:
                new_rows, nb = cache.stats(
                    cleaned.join(states.where(in_batch).select(*KEY_COLUMNS), list(KEY_COLUMNS), "left_anti"),
                    *batch_bounds())
            t_new = time.perf_counter()
            legs_table = spark.read.format("delta").load(LEG_PATH)
            step = stream_step(new_rows.select(*STATE_COLUMNS), states, legs_table, cache, bounds=nb,
                               submit=lambda fn: side.submit(in_session, spark, fn)) if nb["n"] else None
            if step is None:
                events = spark.read.format("delta").load(EVENT_PATH).limit(0)
                leg_rows = legs_table.limit(0)
            else:
                events, leg_rows, _ = step
                events, leg_rows = with_load_time(events), with_load_time(leg_rows)
            events = cache(events)
            leg_rows, lb = cache.stats(leg_rows, F.date_format(F.min("dt"), "yyyy-MM-dd").alias("d_min"))
            t_legs = time.perf_counter()
            latest, latest_s = latest_job.result()
            computed = time.perf_counter()

            # ---- 4. 服务层和湖同时写 ----
            # 湖：事件、航段、最新状态在线程池里并发提交（bronze 已在后台写），状态明细最后
            # 服务层：主线程里一个 PostgreSQL 事务，处理延迟算到它提交为止
            # 两边都按主键幂等、互不依赖，哪一步失败都是整批失败，重启后重放
            tasks = {}
            if cache.size(events) and new_rows is cleaned:
                # 本批都是新快照：起降事件的时间都晚于数据时钟，表里不可能已经有，直接追加。
                # 重放时状态明细还没提交、又走到这里，靠事务标识跳过已经提交过的那次追加
                tasks["事件"] = lambda: timed(lambda: append_rows(events, EVENT_PATH,
                                                                self.txn("dwd_airport_event", batch_id)))
            elif cache.size(events):
                tasks["事件"] = lambda: append_new(spark, events, EVENT_PATH, EVENT_KEY, in_batch,
                                                 self.txn("dwd_airport_event", batch_id))
            if cache.size(leg_rows):
                tasks["航段"] = lambda: with_retry(lambda: replace_rows(
                    spark, leg_rows, LEG_PATH, LEG_KEY, f"dt >= DATE '{lb['d_min']}'", "dt",
                    self.txn("dwd_flight_leg", batch_id)), batch_id, "航段")
            if cache.size(latest):
                tasks["最新"] = lambda: with_retry(lambda: replace_rows(
                    spark, latest, LATEST_PATH, LATEST_KEY, None, None,
                    self.txn("ads_latest_position", batch_id)), batch_id, "最新状态")
            with ThreadPoolExecutor(max_workers=max(len(tasks), 1)) as lake_pool:
                lake_jobs = {name: lake_pool.submit(in_session, spark, fn) for name, fn in tasks.items()}
                sync = pg_sync.sync(
                    self.pg,
                    [(self.specs["latest"], latest), (self.specs["legs"], leg_rows), (self.specs["events"], events)],
                    "batch", clock, batch_id,
                    {"n_records": n_clean, "kafka_ts_min": b["k_min"], "kafka_ts_p50": b["k_p50"]})
                synced = time.perf_counter()
                latency = time.time() - utc_epoch(b["k_min"])
                took = self._gather(batch_id, lake_jobs)
            # 后台写的 bronze：失败就整批失败，重启后重放（各表的写入都幂等）
            n_raw, took["bronze(后台)"] = bronze_job.result()
            # 状态明细最后提交，只追加本批新的行（new_rows 在写任何表之前就算好了）
            t = time.perf_counter()
            if cache.size(new_rows):
                append_rows(new_rows, DWD_STATE_PATH, self.txn("dwd_flight_state", batch_id))
            took["状态"] = time.perf_counter() - t
            done = time.perf_counter()

            self.clock = clock
            self.phases[batch_id] = {
                # 路由到 rt_batch_log 的输入行数用这里记的，而不是 progress.numInputRows：
                # 这条查询里还有一个后台读 Kafka 写 bronze 的源，Spark 的 numInputRows 会把
                # 两个源加在一起，是快照行数的两倍。这里只记本批自己的行数。
                "num_input_rows": n_raw,
                "compute_ms": int((computed - started) * 1000),
                "pg_ms": int((synced - computed) * 1000),
                "delta_ms": int((done - computed) * 1000),
                "sync_seq": sync["seq"],
            }
            same_day = b["t_min"][:10] == b["t_max"][:10]
            span = f"{b['t_min'][11:]}–{b['t_max'][11:]}" if same_day else f"{b['t_min']} – {b['t_max']}"
            parts = "/".join(f"{k} {v:.1f}" for k, v in took.items())
            dedup = "跳过反连接" if new_rows is cleaned else "和表反连接去重"
            print(
                f"[rt] 批 {batch_id}：读入 {n_raw} 条（过滤去重 {n_raw - n_clean}），快照 {span} UTC；"
                f"新状态 {cache.size(new_rows)}（{dedup}），起降事件 {cache.size(events)}，航段 {cache.size(leg_rows)}，"
                f"最新状态 {cache.size(latest)} 行；计算 {computed - started:.1f}s"
                f"（清洗 {t_clean - started:.1f}/新行 {t_new - t_clean:.1f}/航段 {t_legs - t_new:.1f}/"
                f"等最新 {computed - t_legs:.1f}，后台算最新 {latest_s:.1f}），"
                f"写入 {done - computed:.1f}s（PostgreSQL {synced - computed:.2f}s，处理延迟约 {latency:.1f}s；"
                f"湖 {parts}），共 {done - started:.1f}s",
                flush=True,
            )
            self.last_done = batch_id
        finally:
            side.shutdown(wait=True)    # 出错时也等后台写完，再释放它们读的缓存
            cache.release()
            self.busy = False
            self.done.set()

    @staticmethod
    def _gather(batch_id: int, jobs: dict) -> dict:
        """等湖里并发提交的几张表都写完，返回各自耗时。某一张失败就整批失败（等其余写完再抛，重放幂等）。"""
        took, errors = {}, []
        for name, fut in jobs.items():
            try:
                took[name] = fut.result()
            except Exception as e:  # noqa: BLE001 —— 等其他线程结束后统一抛出
                errors.append((name, e))
        if errors:
            name, e = errors[0]
            raise RuntimeError(f"批 {batch_id}：写 {name} 失败，整批会在重启后重放") from e
        return took


class BatchLogListener(StreamingQueryListener):
    """每批结束后，把 Spark 统计的整批耗时和 foreachBatch 里的各段耗时写进 rt_batch_log。

    在监听线程里用单独的连接写，不占批处理的时间；写失败只打印，不影响作业。
    """

    def __init__(self, spark: SparkSession, phases: dict):
        self.client = pg_sync.PgClient(spark, app="rt_pipeline_log")
        self.phases = phases

    def onQueryStarted(self, event) -> None:
        pass

    def onQueryProgress(self, event) -> None:
        p = event.progress
        phase = self.phases.pop(p.batchId, None)
        # 空批也要记一行：没有数据时（例如采集器停了）它就是"作业还活着"的心跳，
        # 告警引擎靠 max(trigger_at) 区分"采集停了"和"流作业停了"。跳过空批的话，
        # 采集一停就会误报成流作业停止。
        row = {
            "query_id": str(p.id), "batch_id": p.batchId, "trigger_at": p.timestamp,
            "num_input_rows": p.numInputRows,
            "trigger_ms": p.durationMs.get("triggerExecution"), "add_batch_ms": p.durationMs.get("addBatch"),
        }
        row.update(phase or {})
        try:
            pg_sync.log_batch(self.client, row)
        except Exception as e:  # noqa: BLE001
            print(f"[rt] 记批次耗时失败（不影响作业）：{e}")

    def onQueryIdle(self, event) -> None:
        pass

    def onQueryTerminated(self, event) -> None:
        self.client.close()


def run_stream(spark: SparkSession, available_now: bool) -> None:
    others = other_streams_running()
    if others:
        sys.exit(f"[rt] 容器里已经有实时作业在跑（进程 {others}），同一时间只能开一个。"
                 "要重启先停掉它：docker exec spark pkill -f rt_pipeline.py")
    stop = StopRequest()
    stop.install(spark)       # 从这里起 Ctrl+C 由 Python 处理：启动阶段收到就在启动完后退出，运行中等当前批写完
    airports = airports_column(load_airports())

    # ---- 湖里的表 ----
    ensure_bronze(spark)
    dwd_state.ensure_table(spark, airports)
    dwd_event_leg.ensure_tables(spark, "rt")          # 结构和代码不一致会退出，并提示删表重建
    clock = data_clock(spark)
    ensure_latest(spark, clock)
    ensure_checkpoint_interval(spark)

    # ---- 服务层：建表，按湖全量对齐 ----
    specs = pg_sync.table_specs(spark.read.format("delta").load(DWD_STATE_PATH).limit(0))
    pg = pg_sync.PgClient(spark)
    pg_sync.ensure_schema(pg, list(specs.values()))
    lake = lake_frames(spark)
    started = time.time()
    full = pg_sync.sync(pg, [(specs[k], lake[k]) for k in ("latest", "legs", "events")], "full", clock)
    print(f"[rt] 服务层已按湖全量对齐：最新状态 {full['rows']['ads_latest_position']} 行、"
          f"航段 {full['rows']['dwd_flight_leg']} 个、起降事件 {full['rows']['dwd_airport_event']} 条，"
          f"数据时钟 {clock} UTC，耗时 {time.time() - started:.1f} 秒")

    # ---- 流 ----
    offsets = starting_offsets(spark)
    stream = to_bronze(
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", offsets)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    batch = RealtimeBatch(airports, specs, clock, pg)
    spark.streams.addListener(BatchLogListener(spark, batch.phases))
    writer = (
        stream.writeStream
        .queryName("rt_pipeline")
        .foreachBatch(batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
        trigger_desc = "availableNow（处理完 Kafka 里的积压就退出）"
    else:
        writer = writer.trigger(processingTime=TRIGGER_INTERVAL)
        trigger_desc = f"每 {TRIGGER_INTERVAL}"

    print("=" * 72)
    print("  实时链路：Kafka → ODS → 状态明细 → 起降事件、航段、最新状态 → PostgreSQL")
    print(f"  读取      : {KAFKA_TOPIC} @ {KAFKA_BOOTSTRAP}（没有 checkpoint 时从 {offsets} 开始）")
    print(f"  写入湖    : {BRONZE_PATH}、{DWD_STATE_PATH}、{EVENT_PATH}、{LEG_PATH}、{LATEST_PATH}")
    print(f"  服务层    : {pg_sync.PG_URL}")
    print(f"  checkpoint: {CHECKPOINT_PATH}")
    print(f"  触发      : {trigger_desc}")
    print("=" * 72, flush=True)
    if stop.requested:
        print("[rt] 启动过程中收到停止信号，不启动流式查询，直接退出。", flush=True)
        pg.close()
        return
    query = writer.start()
    try:
        run_until_stopped(query, batch, stop)
    finally:
        pg.close()
    if available_now and not stop.requested:
        print("[rt] Kafka 里的积压已处理完，退出。", flush=True)


# ================================================================
#  verify
# ================================================================
def verify_latest(spark: SparkSession) -> bool:
    """最新状态表和从状态明细全量重算的结果逐行比（不比 load_time）。"""
    states = spark.read.format("delta").load(DWD_STATE_PATH)
    clock = data_clock(spark)
    current = states.limit(0).withColumn("departed", F.lit(False)).select(*LATEST_COLUMNS)
    expected = latest_updates(current, states, clock)
    actual = spark.read.format("delta").load(LATEST_PATH).select(*LATEST_COLUMNS)
    print(f"  数据时钟 {clock} UTC")
    return dwd_event_leg.compare("最新状态（全表）", expected, actual, LATEST_KEY)


def read_pg(spark: SparkSession, table: str) -> DataFrame:
    return (spark.read.format("jdbc")
            .option("url", pg_sync.PG_URL).option("dbtable", table)
            .option("user", pg_sync.PG_USER).option("password", pg_sync.PG_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load())


def verify_serving(spark: SparkSession) -> bool:
    """服务层三张表和湖里的逐行比。实时作业是先写服务层再写湖，要停掉作业再比。"""
    ok = True
    for table, path, keys in (("ads_latest_position", LATEST_PATH, LATEST_KEY),
                              ("dwd_flight_leg", LEG_PATH, LEG_KEY),
                              ("dwd_airport_event", EVENT_PATH, EVENT_KEY)):
        lake = spark.read.format("delta").load(path).drop("load_time")
        served = read_pg(spark, table).select(*lake.columns)
        ok = dwd_event_leg.compare(f"{table}（湖 vs PostgreSQL）", lake, served, keys) and ok
    return ok


def run_verify(spark: SparkSession, start: str, end: str) -> int:
    airports = airports_column(load_airports())
    results = []
    print("\n#### 1/4 状态明细（按快照时间取区间）")
    results.append(dwd_state.run_verify(spark, airports, start, end) == 0)
    print("\n#### 2/4 起降事件与航段（事件按事件时间、航段按开始时间取区间）")
    results.append(dwd_event_leg.run_verify(spark, start, end) == 0)
    print("\n#### 3/4 最新状态")
    results.append(verify_latest(spark))
    print("\n#### 4/4 服务层（PostgreSQL）与湖")
    results.append(verify_serving(spark))
    names = ["状态明细", "起降事件与航段", "最新状态", "服务层"]
    print("\n" + "=" * 64)
    print("  " + "；".join(f"{n} {'通过' if r else '不一致'}" for n, r in zip(names, results)))
    print("=" * 64)
    return 0 if all(results) else 1


# ================================================================
#  命令行
# ================================================================
def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="实时链路：流式写入 / 对账")
    p.add_argument("mode", nargs="?", default="stream", choices=["stream", "verify"])
    p.add_argument("--available-now", action="store_true", help="stream 模式：处理完 Kafka 里的积压就退出")
    p.add_argument("--start", type=dwd_state.parse_utc, help="verify 区间开始，UTC，包含")
    p.add_argument("--end", type=dwd_state.parse_utc, help="verify 区间结束，UTC，不包含")
    args = p.parse_args(argv)
    if args.mode == "verify":
        if not (args.start and args.end):
            p.error("verify 需要 --start 和 --end")
        if args.start >= args.end:
            p.error("--start 必须早于 --end")
    return args


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    if args.mode == "stream":
        run_stream(spark, args.available_now)
        spark.stop()
    else:
        code = run_verify(spark, args.start, args.end)
        spark.stop()
        sys.exit(code)


if __name__ == "__main__":
    main()
