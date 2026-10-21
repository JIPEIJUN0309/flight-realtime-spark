"""
服务层：实时表同步到 PostgreSQL（一个事务里 COPY + upsert）
=========================================================
实时作业每个微批调用一次 sync：最新状态、航段、起降事件三张表在同一个事务里写完。
Grafana 读的时候要么看到整批之前，要么看到整批之后，不会看到空表或半批。

★ 为什么不用 Spark 的 JDBC 写入（ads_export.py 的做法）
  df.write.jdbc(mode="overwrite") 先删表（或清空），再按分区各自插入、各自提交，
  写入期间看板会读到空表或一半的数据。这里改成：
    1. Spark 把这一批要写的行转成 CSV 文本，收到 driver 上（一批几百到两千行，几百 KB）
    2. driver 上开一个事务：COPY 进临时表 → INSERT … ON CONFLICT 按主键合并 → 记同步序号 → 提交
  一次事务，几十毫秒。

★ 为什么用 JDBC 而不是 psycopg2
  spark 镜像里已经有 PostgreSQL 的 JDBC 驱动（Dockerfile 下载的 postgresql-42.7.10.jar），
  通过 py4j 在 driver 的 JVM 里直接用它，不用往容器里再装 Python 包，也不用重建镜像。

★ 怎么证明不会看到半批
  每一行都带着写入它的同步序号 sync_seq；rt_sync_state 在同一个事务里记下最新序号和这次改了几行。
  探针在一个快照里比对：标着最新序号的行数等于记录的行数，而且表里没有比它更新的序号
  （sql/checks/m3_sla_sampler.sql 持续跑这个比对）。

★ 判定逻辑不在这里
  要写哪些行由上游算好（最新状态见 latest_position.py，事件和航段见 flight_legs.py），
  湖里 MERGE 和这里 upsert 用的是同一组行，这里只负责按主键写入。
"""

import os
import time
from contextlib import contextmanager
from functools import reduce
from typing import Dict, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType,
    StructType, TimestampType,
)

from latest_position import APPROACH, CURRENT_WINDOW_S, DEPARTURE

PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/flightdb")
PG_USER = os.getenv("PG_USER", "flight")
PG_PASSWORD = os.getenv("PG_PASSWORD", "flight123")
# Grafana 用的只读账号：只能读实时表和视图，看不到同库里 Metabase 自己的表
READER_ROLE = os.getenv("PG_READER_ROLE", "grafana_ro")
READER_PASSWORD = os.getenv("PG_READER_PASSWORD", "grafana_ro")

# Spark 类型 → (建表用的类型, information_schema 里显示的类型)
PG_TYPES = {
    StringType: ("text", "text"),
    TimestampType: ("timestamptz", "timestamp with time zone"),
    DoubleType: ("double precision", "double precision"),
    FloatType: ("real", "real"),
    BooleanType: ("boolean", "boolean"),
    DateType: ("date", "date"),
    LongType: ("bigint", "bigint"),
    IntegerType: ("integer", "integer"),
    ShortType: ("smallint", "smallint"),
}

# 每张实时表在上游列之后多两列
META_COLUMNS = [
    ("synced_at", "timestamptz", "timestamp with time zone"),   # 写入服务层的时间
    ("sync_seq", "bigint", "bigint"),                            # 写入它的同步序号
]

# CSV 的写法要和 PostgreSQL 的 COPY … (FORMAT csv) 对上：
#   空值 → 不带引号的空串，COPY 读成 NULL；空字符串 → ""，COPY 读成空字符串
#   引号转义用双写（""），不是 Spark 默认的反斜杠
#   时间带时区（Z），会话时区是 UTC；不去掉首尾空格，原样写入
CSV_OPTIONS = {
    "timestampFormat": "yyyy-MM-dd HH:mm:ss.SSSSSSXXX",
    "dateFormat": "yyyy-MM-dd",
    "quote": '"',
    "escape": '"',
    "nullValue": "",
    "emptyValue": '""',
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
}

STATE_DDL = """
CREATE TABLE IF NOT EXISTS rt_sync_state (
    id             int PRIMARY KEY CHECK (id = 1),
    seq            bigint NOT NULL,          -- 最近一次同步的序号，每个事务加 1
    kind           text,                     -- batch：微批；full：启动时从湖里全量对齐
    batch_id       bigint,                   -- 微批编号（Spark 的 batchId）
    data_clock     timestamptz,              -- 数据时钟：已同步数据里最晚的快照时间
    synced_at      timestamptz,
    latest_changed int,                      -- 这次同步改了几行，探针用来核对
    legs_changed   int,
    events_changed int
);
INSERT INTO rt_sync_state (id, seq) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS rt_sync_log (
    sync_seq       bigint PRIMARY KEY,
    kind           text NOT NULL,
    batch_id       bigint,
    synced_at      timestamptz NOT NULL,     -- 提交前的最后时刻，记作写入服务层的时间
    data_clock     timestamptz,
    n_records      int,                      -- 本批状态明细行数
    kafka_ts_min   timestamptz,
    kafka_ts_p50   timestamptz,
    latency_max_s  double precision,         -- 处理延迟 = 写入服务层的时间 - kafka_ts
    latency_p50_s  double precision,
    latest_changed int,
    legs_changed   int,
    events_changed int
);

CREATE TABLE IF NOT EXISTS rt_batch_log (
    query_id       text NOT NULL,            -- 流式查询的 id，checkpoint 不删就不变
    batch_id       bigint NOT NULL,
    trigger_at     timestamptz,              -- 这一批开始的时间
    num_input_rows bigint,                   -- 读到的 Kafka 消息数
    trigger_ms     int,                      -- Spark 统计的整批耗时（含读 offset、写 checkpoint）
    add_batch_ms   int,                      -- 其中 foreachBatch 的耗时
    compute_ms     int,                      -- foreachBatch 里：清洗、判起降、切航段、算最新状态
    pg_ms          int,                      --                 写 PostgreSQL（含转 CSV）
    delta_ms       int,                      --                 并发提交 Delta
    sync_seq       bigint,
    PRIMARY KEY (query_id, batch_id)
);
"""


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def ts_literal(utc_text: str) -> str:
    """UTC 时间字符串 → PostgreSQL 的 timestamptz 常量。"""
    return f"TIMESTAMPTZ '{utc_text}+00'"


class TableSpec:
    """一张实时表：列取自上游 DataFrame 的结构，主键和湖里的表一致。"""

    def __init__(self, name: str, schema: StructType, key: Sequence[str],
                 indexes: Sequence[str] = (), insert_only: bool = False):
        self.name = name
        self.fields = list(schema.fields)
        self.columns = [f.name for f in self.fields]
        self.key = list(key)
        self.indexes = list(indexes)
        # 起降事件和湖里一样只插入：同一事件重放时不改已有的行
        self.insert_only = insert_only
        missing = [k for k in self.key if k not in self.columns]
        if missing:
            raise ValueError(f"{name} 的主键列不在表结构里：{missing}")

    @property
    def stage(self) -> str:
        return f"_stage_{self.name}"

    def _pg_type(self, field, idx: int) -> str:
        for spark_type, types in PG_TYPES.items():
            if isinstance(field.dataType, spark_type):
                return types[idx]
        raise TypeError(f"{self.name}.{field.name} 的类型 {field.dataType} 没有对应的 PostgreSQL 类型")

    def expected_columns(self) -> List[tuple]:
        return ([(f.name, self._pg_type(f, 1)) for f in self.fields]
                + [(name, info) for name, _, info in META_COLUMNS])

    def create_sql(self) -> str:
        cols = [f"{quote(f.name)} {self._pg_type(f, 0)}" + (" NOT NULL" if f.name in self.key else "")
                for f in self.fields]
        cols += [f"{quote(name)} {ddl} NOT NULL" for name, ddl, _ in META_COLUMNS]
        key = ", ".join(quote(k) for k in self.key)
        return f"CREATE TABLE IF NOT EXISTS {self.name} ({', '.join(cols)}, PRIMARY KEY ({key}))"

    def index_sql(self) -> List[str]:
        stmts = [f"CREATE INDEX IF NOT EXISTS {self.name}_sync_seq ON {self.name} (sync_seq)"]
        for i, cols in enumerate(self.indexes):
            stmts.append(f"CREATE INDEX IF NOT EXISTS {self.name}_ix{i} ON {self.name} ({cols})")
        return stmts

    def stage_sql(self) -> str:
        # 临时表只在本连接可见；ON COMMIT DELETE ROWS：每次提交后自动清空，下一批从空表开始
        cols = ", ".join(f"{quote(f.name)} {self._pg_type(f, 0)}" for f in self.fields)
        return f"CREATE TEMP TABLE IF NOT EXISTS {self.stage} ({cols}) ON COMMIT DELETE ROWS"

    def upsert_sql(self, seq: int, update_existing: bool) -> str:
        cols = ", ".join(quote(c) for c in self.columns)
        key = ", ".join(quote(k) for k in self.key)
        if update_existing:
            sets = ", ".join(f"{quote(c)} = EXCLUDED.{quote(c)}"
                             for c in self.columns + ["synced_at", "sync_seq"] if c not in self.key)
            action = f"DO UPDATE SET {sets}"
        else:
            action = "DO NOTHING"
        return (f"INSERT INTO {self.name} ({cols}, synced_at, sync_seq) "
                f"SELECT {cols}, clock_timestamp(), {int(seq)} FROM {self.stage} "
                f"ON CONFLICT ({key}) {action}")

    def delete_missing_sql(self) -> str:
        """全量对齐时删掉湖里已经没有的行（例如航段 rebuild 之后旧的 leg_id）。"""
        match = " AND ".join(f"s.{quote(k)} = t.{quote(k)}" for k in self.key)
        return f"DELETE FROM {self.name} t WHERE NOT EXISTS (SELECT 1 FROM {self.stage} s WHERE {match})"


class PgClient:
    """driver 上的一个 JDBC 连接（经 py4j 调用 JVM 里的 PostgreSQL 驱动）。不跨线程共用。"""

    def __init__(self, spark: SparkSession, url: str = PG_URL, user: str = PG_USER,
                 password: str = PG_PASSWORD, app: str = "rt_pipeline", autocommit: bool = False):
        self._jvm = spark._jvm
        self.url, self.user, self.password, self.app = url, user, password, app
        self.autocommit = autocommit
        self._conn = None
        self._staged = set()

    def connect(self):
        if self._conn is not None and not self._conn.isClosed():
            return self._conn
        props = self._jvm.java.util.Properties()
        props.setProperty("user", self.user)
        props.setProperty("password", self.password)
        props.setProperty("ApplicationName", self.app)
        props.setProperty("connectTimeout", "10")
        # 直接用驱动类连接，不经过 DriverManager：驱动不在系统类加载器里时 DriverManager 会找不到它
        conn = self._jvm.org.postgresql.Driver().connect(self.url, props)
        if conn is None:
            raise RuntimeError(f"PostgreSQL JDBC 地址不对：{self.url}")
        conn.setAutoCommit(self.autocommit)
        self._conn = conn
        self._staged = set()     # 新连接上临时表要重建
        return conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001 —— 关闭失败不影响后续重连
                pass
        self._conn = None

    def execute(self, sql: str) -> int:
        """执行一条语句，返回影响行数（DDL 返回 0）。"""
        stmt = self.connect().createStatement()
        try:
            stmt.execute(sql)
            return max(stmt.getUpdateCount(), 0)
        finally:
            stmt.close()

    def query(self, sql: str) -> List[tuple]:
        """执行查询，每个值都按字符串取回（不经过 Python 的 datetime，避免时区换算）。"""
        stmt = self.connect().createStatement()
        try:
            rs = stmt.executeQuery(sql)
            n = rs.getMetaData().getColumnCount()
            rows = []
            while rs.next():
                rows.append(tuple(rs.getString(i) for i in range(1, n + 1)))
            return rows
        finally:
            stmt.close()

    def copy_in(self, table: str, columns: Sequence[str], lines: Sequence[str]) -> int:
        if not lines:
            return 0
        manager = self._jvm.org.postgresql.copy.CopyManager(self.connect())
        cols = ", ".join(quote(c) for c in columns)
        reader = self._jvm.java.io.StringReader("\n".join(lines) + "\n")
        return int(manager.copyIn(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT csv)", reader))

    def ensure_stage(self, spec: TableSpec) -> None:
        if spec.name not in self._staged:
            self.execute(spec.stage_sql())
            self._staged.add(spec.name)

    @contextmanager
    def transaction(self):
        """出错就回滚并断开（下次重连），异常照常抛出；正常结束就提交。"""
        conn = self.connect()
        try:
            yield self
            conn.commit()
        except BaseException:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001 —— 连接已断时回滚也会失败，直接断开重连
                pass
            self.close()
            raise


def table_specs(states: DataFrame) -> Dict[str, TableSpec]:
    """三张实时表的结构，直接取上游计算函数的输出（传入状态明细，空表即可）：代码和表结构不会对不上。

    湖里的表多一列 load_time（写入 Delta 的时间），这里不要；PostgreSQL 另有 synced_at 和 sync_seq。
    """
    from flight_legs import EVENT_KEY, LEG_KEY, compute_all
    from latest_position import LATEST_COLUMNS, LATEST_KEY, latest_updates

    empty = states.limit(0)
    events, legs = compute_all(empty, "1970-01-01 00:00:00")
    current = empty.withColumn("departed", F.lit(False)).select(*LATEST_COLUMNS)
    latest = latest_updates(current, empty, "1970-01-01 00:00:00")
    return {
        "latest": TableSpec("ads_latest_position", latest.schema, LATEST_KEY,
                            indexes=["near_airport_iata"]),
        "legs": TableSpec("dwd_flight_leg", legs.schema, LEG_KEY,
                          indexes=["status", "icao24, start_time"]),
        "events": TableSpec("dwd_airport_event", events.schema, EVENT_KEY,
                            indexes=["airport_iata, event_time"], insert_only=True),
    }


# ================================================================
#  建表
# ================================================================
def existing_columns(client: PgClient, table: str) -> List[tuple]:
    return client.query(
        "SELECT column_name, data_type FROM information_schema.columns "
        f"WHERE table_schema = current_schema() AND table_name = '{table}' ORDER BY ordinal_position")


def primary_key(client: PgClient, table: str) -> List[str]:
    rows = client.query(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        f"WHERE i.indrelid = to_regclass('{table}') AND i.indisprimary")
    return sorted(r[0] for r in rows)


def ensure_schema(client: PgClient, specs: Sequence[TableSpec], log=print) -> None:
    """建实时表、元数据表、视图和只读账号。

    表已存在但结构和代码不一致时（例如批链路以前用 JDBC 整表覆盖写的 ads_latest_position，
    没有主键），删掉重建。实时表都能从湖里全量对齐回来（full_sync），所以重建是安全的；
    删表、建表、建视图在一个事务里，读者要么看到旧表，要么看到新表。
    """
    names = sorted(s.name for s in specs)
    if names != sorted(CHANGED_COLUMNS):
        raise ValueError(f"实时表应为 {sorted(CHANGED_COLUMNS)}，传入的是 {names}")
    with client.transaction():
        for stmt in _split(STATE_DDL):
            client.execute(stmt)
        for view in VIEWS:
            client.execute(f"DROP VIEW IF EXISTS {view}")
        for spec in specs:
            actual = existing_columns(client, spec.name)
            if actual:
                expected = spec.expected_columns()
                pk = primary_key(client, spec.name)
                if [tuple(c) for c in actual] != expected or pk != sorted(spec.key):
                    log(f"[pg] {spec.name} 的结构和代码不一致（{_diff(actual, expected, pk, spec.key)}），"
                        "删掉重建，随后从湖里全量对齐")
                    client.execute(f"DROP TABLE {spec.name} CASCADE")
            client.execute(spec.create_sql())
            for stmt in spec.index_sql():
                client.execute(stmt)
        client.execute(CURRENT_VIEW_SQL)
        client.execute(ATOMICITY_VIEW_SQL)
        _grant_reader(client, [s.name for s in specs])


def _diff(actual, expected, pk, key) -> str:
    a, e = [tuple(c) for c in actual], expected
    parts = []
    added = [n for n, _ in e if n not in dict(a)]
    removed = [n for n, _ in a if n not in dict(e)]
    changed = [n for n, t in e if n in dict(a) and dict(a)[n] != t]
    if added:
        parts.append("缺少列 " + "、".join(added))
    if removed:
        parts.append("多出列 " + "、".join(removed))
    if changed:
        parts.append("类型不同 " + "、".join(changed))
    if pk != sorted(key):
        parts.append(f"主键是 {pk or '无'}，应为 {list(key)}")
    return "；".join(parts) or "列的顺序不同"


def _split(sql: str) -> List[str]:
    return [s.strip() for s in sql.split(";") if s.strip()]


# 每张实时表在 rt_sync_state 里对应的"这次改了几行"列
CHANGED_COLUMNS = {
    "ads_latest_position": "latest_changed",
    "dwd_flight_leg": "legs_changed",
    "dwd_airport_event": "events_changed",
}

# 当前在场的飞机：没离开、最后出现在数据时钟之前 30 秒以内。进近、离场标记直接用 latest_position 的
# 表达式（rt_sync_state 没有同名列，不用加表别名），看板只数这两个标记，口径只写在一处
CURRENT_VIEW_SQL = f"""
CREATE VIEW rt_current_position AS
SELECT p.*,
       COALESCE({APPROACH}, FALSE)  AS is_approach,
       COALESCE({DEPARTURE}, FALSE) AS is_departure
FROM ads_latest_position p
CROSS JOIN rt_sync_state s
WHERE NOT p.departed
  AND p.event_time >= s.data_clock - INTERVAL '{CURRENT_WINDOW_S} seconds'
"""

# 原子性检查：一条 SELECT 只用一个快照，几个子查询看到的是同一时刻的数据。
#   seq_complete   标着最新序号的行数 = 那次同步记下的改动行数（看到的是完整的一批）
#   no_newer_rows  表里没有比最新序号更新的行（没有提前看到下一批的一部分）
# 写入是一个事务的话，这两项永远为真；探针每 0.2 秒查一次（sql/checks/m3_sla_sampler.sql）
ATOMICITY_VIEW_SQL = """
CREATE VIEW rt_atomicity_check AS
SELECT s.seq,
       s.kind,
       (SELECT count(*) FROM ads_latest_position) AS latest_rows,
       (SELECT count(*) FROM dwd_flight_leg)      AS leg_rows,
       (SELECT count(*) FROM dwd_airport_event)   AS event_rows,
       (SELECT count(*) FROM ads_latest_position WHERE sync_seq = s.seq) = COALESCE(s.latest_changed, 0)
       AND (SELECT count(*) FROM dwd_flight_leg WHERE sync_seq = s.seq) = COALESCE(s.legs_changed, 0)
       AND (SELECT count(*) FROM dwd_airport_event WHERE sync_seq = s.seq) = COALESCE(s.events_changed, 0)
                                                  AS seq_complete,
       COALESCE(GREATEST((SELECT max(sync_seq) FROM ads_latest_position),
                         (SELECT max(sync_seq) FROM dwd_flight_leg),
                         (SELECT max(sync_seq) FROM dwd_airport_event)) <= s.seq, TRUE)
                                                  AS no_newer_rows
FROM rt_sync_state s
WHERE s.id = 1
"""

VIEWS = ["rt_current_position", "rt_atomicity_check"]


def _grant_reader(client: PgClient, tables: Sequence[str]) -> None:
    client.execute(f"""
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{READER_ROLE}') THEN
        CREATE ROLE {READER_ROLE} LOGIN PASSWORD '{READER_PASSWORD}';
    END IF;
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO {READER_ROLE}', current_database());
END
$$""")
    objects = list(tables) + VIEWS + ["rt_sync_state", "rt_sync_log", "rt_batch_log"]
    client.execute(f"GRANT USAGE ON SCHEMA public TO {READER_ROLE}")
    client.execute(f"GRANT SELECT ON {', '.join(objects)} TO {READER_ROLE}")


# ================================================================
#  同步
# ================================================================
def csv_lines(frames: Dict[str, DataFrame]) -> Dict[str, List[str]]:
    """几张表的行一起转成 CSV 收到 driver：只跑一个 Spark 作业。"""
    parts = [
        df.select(F.lit(name).alias("_t"), F.to_csv(F.struct(*df.columns), CSV_OPTIONS).alias("_line"))
        for name, df in frames.items()
    ]
    out = {name: [] for name in frames}
    if parts:
        for row in reduce(DataFrame.unionByName, parts).collect():
            out[row["_t"]].append(row["_line"])
    return out


def _counts_sql(seq: int) -> Dict[str, str]:
    # 三张表都数：这次没写的表计 0，探针核对时每张表都对得上
    return {col: f"(SELECT count(*) FROM {table} WHERE sync_seq = {int(seq)})"
            for table, col in CHANGED_COLUMNS.items()}


def sync(client: PgClient, items: Sequence[tuple], kind: str, clock: Optional[str],
         batch_id: Optional[int] = None, stats: Optional[dict] = None) -> dict:
    """把几张表的行写进 PostgreSQL，一个事务。

      items  [(TableSpec, DataFrame), ...]：要写的行，列和 TableSpec 一致
      kind   'batch'：微批，按主键 upsert（事件只插入）；
             'full' ：全量对齐，湖里有的 upsert、湖里没有的删掉
      clock  数据时钟（UTC 字符串）；微批只会把 rt_sync_state 的时钟往后推
      stats  微批的 n_records、kafka_ts_min、kafka_ts_p50（UTC 字符串），用来算处理延迟

    返回 {seq, rows: {表名: 写入行数}, csv_ms, tx_ms}。
    """
    started = time.perf_counter()
    # 按表结构的列顺序转 CSV（COPY 按列名对应）；多出来的列（例如湖里的 load_time）不写。Spark 作业放在事务外
    lines = csv_lines({spec.name: df.select(*spec.columns) for spec, df in items})
    csv_done = time.perf_counter()
    specs = [spec for spec, _ in items]
    written = {}
    with client.transaction():
        # 锁住状态行再取序号：即使误开了两个实时作业，序号也不会重复
        seq = int(client.query("SELECT seq + 1 FROM rt_sync_state WHERE id = 1 FOR UPDATE")[0][0])
        for spec in specs:
            client.ensure_stage(spec)
            client.copy_in(spec.stage, spec.columns, lines[spec.name])
            if kind == "full":
                client.execute(spec.delete_missing_sql())
            update = kind == "full" or not spec.insert_only
            written[spec.name] = client.execute(spec.upsert_sql(seq, update))

        counts = _counts_sql(seq)
        if kind == "full":
            clock_sql = ts_literal(clock) if clock else "NULL"
        else:
            clock_sql = (f"GREATEST(COALESCE(data_clock, '-infinity'::timestamptz), {ts_literal(clock)})"
                         if clock else "data_clock")
        assignments = ", ".join([
            f"seq = {seq}", f"kind = '{kind}'",
            f"batch_id = {int(batch_id) if batch_id is not None else 'NULL'}",
            f"data_clock = {clock_sql}", "synced_at = clock_timestamp()",
            *[f"{col} = {expr}" for col, expr in counts.items()],
        ])
        client.execute(f"UPDATE rt_sync_state SET {assignments} WHERE id = 1")

        s = stats or {}
        k_min = ts_literal(s["kafka_ts_min"]) if s.get("kafka_ts_min") else "NULL"
        k_p50 = ts_literal(s["kafka_ts_p50"]) if s.get("kafka_ts_p50") else "NULL"
        client.execute(f"""
INSERT INTO rt_sync_log (sync_seq, kind, batch_id, synced_at, data_clock, n_records,
                         kafka_ts_min, kafka_ts_p50, latency_max_s, latency_p50_s,
                         latest_changed, legs_changed, events_changed)
SELECT {seq}, '{kind}', {int(batch_id) if batch_id is not None else 'NULL'}, clock_timestamp(),
       st.data_clock, {int(s.get('n_records') or 0)}, {k_min}, {k_p50},
       EXTRACT(EPOCH FROM clock_timestamp() - {k_min}),
       EXTRACT(EPOCH FROM clock_timestamp() - {k_p50}),
       st.latest_changed, st.legs_changed, st.events_changed
FROM rt_sync_state st WHERE st.id = 1""")
    done = time.perf_counter()
    return {"seq": seq, "rows": written,
            "csv_ms": int((csv_done - started) * 1000), "tx_ms": int((done - csv_done) * 1000)}


def log_batch(client: PgClient, row: dict) -> None:
    """记一条微批耗时（由流式查询的监听器在每批结束后调用），自动提交。"""
    cols = ["query_id", "batch_id", "trigger_at", "num_input_rows", "trigger_ms", "add_batch_ms",
            "compute_ms", "pg_ms", "delta_ms", "sync_seq"]

    def lit(c):
        v = row.get(c)
        if v is None:
            return "NULL"
        if c == "query_id":
            return f"'{v}'"
        if c == "trigger_at":
            return f"TIMESTAMPTZ '{v}'"
        return str(int(v))

    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols[2:])
    with client.transaction():
        client.execute(f"INSERT INTO rt_batch_log ({', '.join(cols)}) VALUES ({', '.join(lit(c) for c in cols)}) "
                       f"ON CONFLICT (query_id, batch_id) DO UPDATE SET {sets}")
