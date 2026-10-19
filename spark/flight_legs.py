"""
起降事件与航段：流批共用的唯一实现
==================================
从状态明细 dwd_flight_state 推出两张表：

  dwd_airport_event  一次起飞或落地，主键 (icao24, event_time, event_type)
  dwd_flight_leg     一个航段（累积快照），主键 leg_id

口径以 docs/requirements.md「维度与通用规则 → 起降判定 / 航段切分」为准：改规则先改文档，再改这里。
流式作业、全量重建和对账（dwd_event_leg.py）都调用这里的函数，单测也直接测这些函数。
这里只做计算，不读写 Delta，所以本地不装 Delta 也能把流式的分批过程完整模拟一遍。

★ 起降为什么要自己推
  OpenSky 只给状态快照，没有"落地了"这样的事件。同一架飞机相邻两条记录从空中变成地面
  就是落地，反过来是起飞；两条间隔超过 1 分钟、或翻转后离机场超过 5 km 都不算。
  "地面"先修正过滑行抖动：机场 5 km 内、时速低于 100 km/h 的记录即使 on_ground 为 false
  也算在地面（见 GROUND_STATE）。

★ 航段丢了，丢在哪
  LOST 的航段按最后一条空中记录分三种原因：疑似落地未识别、飞出范围、信号丢失（见 LOST_REASON）。
  报表看漏判多少落地，就看第一种。

★ 流式和批量重算怎么保证结果一样
  1. 判定只看记录本身和它前面的记录，不看机器时间。批量重算一次看全部记录；流式每批看本批
     记录，再回看这批飞机各自最近 20 分钟的记录，够判断翻转、航段是否延续、起飞机场。
  2. 航段开始得比回看窗口还早时（长航段），它的 leg_id、开始时间、起飞机场从航段表里取。
     这几个字段写入后就不再变，所以重放时读到的还是同一个值。
  3. 航段合并只用单调的规则：最后出现时间和最大高度取较大值，结束状态按 LANDED > LOST > IN_AIR
     取高的。同一批数据处理一次还是重放几次，结果都一样。
  4. 判超时用的"现在"是数据时间，即本批最晚的快照时间，不是机器时间。

要求 Spark 会话时区是 UTC：dt 取 UTC 日期，常量时间按 UTC 解释。
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# ================================================================
#  规则参数（和需求文档"待校准参数"一致）
# ================================================================
FLIP_MAX_GAP_S = 60          # 相邻两条间隔超过它就不判翻转：中间那段没看到，不知道几时起降
EVENT_MAX_DIST_KM = 5.0      # 翻转后那条离最近机场不超过它才算起降；也是滑行抖动修正的范围
TAXI_MAX_SPEED_KMH = 100.0   # 机场 5 km 内低于这个时速的记录算在地面
LEG_GAP_S = 20 * 60          # 航段断开间隔；也是判断"之前 20 分钟内有没有记录"的窗口和流式回看长度

SUSPECT_LANDING_DIST_KM = 10.0     # 疑似落地未识别：最后一条空中记录离机场不超过它……
SUSPECT_LANDING_MAX_ALT_M = 1000.0  # ……气压高度低于它，且在下降
EXIT_EDGE_KM = 20.0                # 飞出范围：最后一条空中记录离采集范围边界不超过它
EARTH_RADIUS_KM = 6371.0           # 和 flight_rules 的 Haversine 用同一个半径

# 采集范围的历史：(生效时间 UTC, 纬度下限, 纬度上限, 经度下限, 经度上限)，按时间先后排。
# 判断"飞出范围"要用记录当时的范围。采集器的 BBOX_* 改了，就在这里加一行，并同步需求文档
COVERAGE = [
    ("1970-01-01 00:00:00", 45.0, 50.0, 5.0, 10.0),    # 7 月的历史数据：25 平方度
    ("2026-10-17 00:00:00", 43.0, 52.0, 2.5, 12.5),    # M0 起：90 平方度
]

TS_FMT = "yyyy-MM-dd HH:mm:ss"

# 从状态明细里用到的列
STATE_COLUMNS = [
    "icao24", "event_time", "on_ground", "callsign", "latitude", "longitude",
    "altitude_m", "speed_kmh", "flight_phase", "near_airport_iata", "near_airport_name",
    "near_airport_city", "airport_dist_km", "kafka_ts",
]

# dwd_airport_event 的列，顺序就是表结构；主键和时间放前面，Delta 才会给它们收集 min/max 统计
EVENT_COLUMNS = [
    "icao24",
    "event_time",                           # 翻转后那条记录的快照时间（UTC）
    "event_type",                           # LANDING / TAKEOFF
    "near_airport_iata AS airport_iata",
    "near_airport_name AS airport_name",
    "near_airport_city AS airport_city",
    "airport_dist_km",
    "callsign",
    "latitude",
    "longitude",
    "altitude_m",
    "speed_kmh",
    "prev_time AS prev_event_time",         # 翻转前那条记录的时间
    "gap_s",                                # 两条记录的间隔（秒）
    "kafka_ts",                             # 翻转后那条写入 Kafka 的时间，算处理延迟用
    "DATE(event_time) AS dt",               # UTC 日期分区
]
EVENT_KEY = ("icao24", "event_time", "event_type")

# dwd_flight_leg 的列
LEG_COLUMNS = [
    "leg_id",            # icao24 + "_" + 第一条空中记录的 UTC 秒
    "icao24",
    "start_time",        # 第一条空中记录的时间
    "last_seen",         # 最后一条空中记录的时间；地面记录不更新它
    "end_time",          # LANDED 取落地事件时间，LOST 取 last_seen，进行中为空
    "status",            # IN_AIR / LANDED / LOST
    "lost_reason",       # 只有 LOST 有：SUSPECTED_LANDING 疑似落地未识别 / EXITED 飞出范围 / SIGNAL_LOST 信号丢失
    "departure_kind",    # TAKEOFF 起飞事件 / UNDETECTED 未知（范围内起飞没判出来）/ OUTSIDE 区域外起飞
    "departure_iata",    # 只有 TAKEOFF 才有
    "departure_name",
    "departure_city",
    "arrival_iata",      # 只有 LANDED 才有
    "arrival_name",
    "arrival_city",
    "callsign",          # 以下七列取最后一条空中记录，丢失原因也按它们判断
    "last_latitude",
    "last_longitude",
    "last_altitude_m",
    "last_flight_phase",
    "last_airport_iata",       # 最近的机场
    "last_airport_dist_km",
    "max_altitude_m",
    "dt",                # start_time 的 UTC 日期
]
LEG_KEY = ("leg_id",)

# ================================================================
#  规则一：起降判定
# ================================================================
# 判定用的地面状态。ADS-B 的地面标记在滑行、停机时偶尔跳成"空中"，不修正会多出一对假起降和
# 一个几分钟长的假航段。真起飞的离地时速在 200 km/h 以上（2026-10-19 实测正常起飞中位数
# 259 km/h），滑行只有几十 km/h，所以机场 5 km 内低于 100 km/h 的一律算地面。速度为空时不改。
# 只在这里修正，状态明细的 on_ground 保持原值
GROUND_STATE = f"""
    on_ground OR COALESCE(
        airport_dist_km <= {EVENT_MAX_DIST_KM} AND speed_kmh < {TAXI_MAX_SPEED_KMH}, FALSE)
"""

EVENT_TYPE = f"""
    CASE
        WHEN prev_time IS NULL OR gap_s > {FLIP_MAX_GAP_S}                    THEN NULL
        WHEN airport_dist_km IS NULL OR airport_dist_km > {EVENT_MAX_DIST_KM} THEN NULL
        WHEN NOT prev_on_ground AND on_ground                                 THEN 'LANDING'
        WHEN prev_on_ground AND NOT on_ground                                 THEN 'TAKEOFF'
    END
"""

# ================================================================
#  规则二：航段切分（只对 _decide 的行判断；流式时回看窗口里的行只作参照）
# ================================================================
# 开始一个新航段：起飞事件；或者没有可以延续的航段——20 分钟内没有空中记录，或者上一条
# 空中记录之后发生过落地
LEG_START = f"""
    _decide AND NOT on_ground AND (
        COALESCE(event_type = 'TAKEOFF', FALSE)
        OR prev_air_time IS NULL
        OR unix_timestamp(event_time) - unix_timestamp(prev_air_time) > {LEG_GAP_S}
        OR _landings > COALESCE(_landings_at_prev_air, 0)
    )
"""

# 起飞机场：有起飞事件取事件的机场；前一条记录在地面且在 20 分钟内，说明在范围内起飞但没判出来
DEPARTURE_KIND = f"""
    CASE
        WHEN NOT is_leg_start                                                  THEN NULL
        WHEN COALESCE(event_type = 'TAKEOFF', FALSE)                           THEN 'TAKEOFF'
        WHEN prev_time IS NOT NULL AND prev_on_ground AND gap_s <= {LEG_GAP_S} THEN 'UNDETECTED'
        ELSE 'OUTSIDE'
    END
"""

LEG_ID = """
    CASE WHEN leg_start_time IS NOT NULL
         THEN concat(icao24, '_', CAST(unix_timestamp(leg_start_time) AS STRING))
    END
"""


# ================================================================
#  规则三：丢失原因（只看航段行自己的列，流式和批量算出来一样）
# ================================================================
def _coverage_at(ts_col: str, i: int) -> str:
    """ts_col 那一刻生效的采集范围的第 i 项（1 纬度下限、2 纬度上限、3 经度下限、4 经度上限）。"""
    if len(COVERAGE) == 1:
        return f"{COVERAGE[0][i]}D"
    later = " ".join(f"WHEN {ts_col} >= TIMESTAMP '{c[0]}' THEN {c[i]}D" for c in reversed(COVERAGE[1:]))
    return f"(CASE {later} ELSE {COVERAGE[0][i]}D END)"


# 最后一条空中记录离采集范围最近一条边的距离（km）。范围只有几度，按经纬度差换算足够准：
# 纬度一度约 111 km，经度一度再乘 cos(纬度)
EDGE_DIST_KM = f"""
    {EARTH_RADIUS_KM} * LEAST(
        RADIANS(last_latitude - {_coverage_at("last_seen", 1)}),
        RADIANS({_coverage_at("last_seen", 2)} - last_latitude),
        RADIANS(last_longitude - {_coverage_at("last_seen", 3)}) * COS(RADIANS(last_latitude)),
        RADIANS({_coverage_at("last_seen", 4)} - last_longitude) * COS(RADIANS(last_latitude))
    )
"""

# 依次判断，先满足的为准：
#   疑似落地未识别  离机场 10 km 内、低于 1,000 m、在下降。要求在下降，是为了排除刚起飞就收不到信号的离场
#   飞出范围        离当时的采集范围边界 20 km 内
#   信号丢失        其余：落在维表外的机场、低空收不到信号、应答机关闭等
LOST_REASON = f"""
    CASE
        WHEN status <> 'LOST'                                         THEN NULL
        WHEN last_airport_dist_km <= {SUSPECT_LANDING_DIST_KM}
         AND last_altitude_m < {SUSPECT_LANDING_MAX_ALT_M}
         AND last_flight_phase = 'DESCENDING'                         THEN 'SUSPECTED_LANDING'
        WHEN {EDGE_DIST_KM} <= {EXIT_EDGE_KM}                         THEN 'EXITED'
        ELSE 'SIGNAL_LOST'
    END
"""


def _by_aircraft():
    # 窗口要在有 SparkSession 时才能建，所以不放在模块级
    return Window.partitionBy("icao24").orderBy("event_time")


def with_flips(states: DataFrame) -> DataFrame:
    """给每条记录加上前一条记录的时间、地面状态、间隔，以及起降类型（没有就是空）。

    on_ground 在这里换成判定用的地面状态（GROUND_STATE），后面的起降和航段规则都用它。
    """
    w = _by_aircraft()
    return (
        states
        .withColumn("on_ground", F.expr(GROUND_STATE))
        .withColumn("prev_time", F.lag("event_time").over(w))
        .withColumn("prev_on_ground", F.lag("on_ground").over(w))
        .withColumn("gap_s", F.expr("unix_timestamp(event_time) - unix_timestamp(prev_time)"))
        .withColumn("event_type", F.expr(EVENT_TYPE))
    )


def events_from(flipped: DataFrame) -> DataFrame:
    """取出起降事件，列和 dwd_airport_event 一致。"""
    require_utc(flipped)
    return flipped.where("event_type IS NOT NULL").selectExpr(*EVENT_COLUMNS)


def mark_leg_starts(flipped: DataFrame) -> DataFrame:
    """标出新航段的第一条记录（is_leg_start），以及它的起飞机场类型。"""
    w = _by_aircraft()
    before = w.rowsBetween(Window.unboundedPreceding, -1)
    upto = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    is_air = ~F.col("on_ground")
    return (
        flipped
        # 截至当前行一共有几次落地。拿它和上一条空中记录时的值比，就知道中间有没有落地
        .withColumn("_landings",
                    F.sum(F.when(F.col("event_type") == "LANDING", 1).otherwise(0)).over(upto))
        .withColumn("prev_air_time", F.last(F.when(is_air, F.col("event_time")), ignorenulls=True).over(before))
        .withColumn("_landings_at_prev_air",
                    F.last(F.when(is_air, F.col("_landings")), ignorenulls=True).over(before))
        .withColumn("is_leg_start", F.expr(LEG_START))
        .withColumn("departure_kind", F.expr(DEPARTURE_KIND))
    )


def with_legs(marked: DataFrame, context: DataFrame = None) -> DataFrame:
    """给每条记录标上它所在航段的开始时间和 leg_id。

    航段从最近一个 is_leg_start 开始。流式时，本批第一个新航段之前的记录延续的是回看窗口
    之前就开始的航段，它的开始时间从 context（航段表）里取。
    """
    upto = _by_aircraft().rowsBetween(Window.unboundedPreceding, Window.currentRow)
    own_start = F.last(F.when(F.col("is_leg_start"), F.col("event_time")), ignorenulls=True).over(upto)
    df = marked.withColumn("_own_start", own_start)
    if context is None:
        df = df.withColumn("leg_start_time", F.col("_own_start"))
    else:
        ctx = context.select("icao24", F.col("start_time").alias("_ctx_start"))
        df = (df.join(ctx, "icao24", "left")
              .withColumn("leg_start_time", F.coalesce("_own_start", "_ctx_start"))
              .drop("_ctx_start"))
    return df.withColumn("leg_id", F.expr(LEG_ID))


def leg_updates(assigned: DataFrame, context: DataFrame, clock: str) -> DataFrame:
    """按航段汇总 _decide 的行，得到航段行（列同 LEG_COLUMNS）。

    批量重算时 context 为 None，汇总的就是完整航段。流式时只汇总本批记录，是增量：
    同一个 leg_id 要用 apply_leg_updates 和表里的行合并。

    状态：本段有落地事件 → LANDED；后面又开了新航段 → LOST；最后一段超过 20 分钟没有
    空中记录 → LOST（clock 是数据时钟，UTC 字符串）；否则 IN_AIR。
    """
    rows = assigned.where("_decide AND leg_id IS NOT NULL")
    is_takeoff = F.col("departure_kind") == "TAKEOFF"

    agg = (
        rows.where("NOT on_ground")
        .groupBy("leg_id")
        .agg(
            F.max("event_time").alias("_agg_last_seen"),
            F.max("altitude_m").alias("_agg_max_alt"),
            # 同一架飞机同一时刻只有一条记录，按时间取最后一条是确定的
            F.max_by(F.struct("callsign", "latitude", "longitude", "altitude_m", "flight_phase",
                              "near_airport_iata", "airport_dist_km"), "event_time").alias("_last"),
            # 每个航段最多一条开始记录；本批没有开始记录的，起飞机场从 context 取
            F.max(F.when(F.col("is_leg_start"), F.struct(
                F.col("departure_kind").alias("kind"),
                F.when(is_takeoff, F.col("near_airport_iata")).alias("iata"),
                F.when(is_takeoff, F.col("near_airport_name")).alias("name"),
                F.when(is_takeoff, F.col("near_airport_city")).alias("city"),
            ))).alias("_dep"),
        )
    )
    # 落地之后的空中记录一定会开新航段，所以一个航段最多一次落地
    landings = (
        rows.where("event_type = 'LANDING'")
        .groupBy("leg_id")
        .agg(F.min_by(F.struct(
            F.col("event_time").alias("time"),
            F.col("near_airport_iata").alias("iata"),
            F.col("near_airport_name").alias("name"),
            F.col("near_airport_city").alias("city"),
        ), "event_time").alias("_land"))
    )
    latest_start = rows.where("is_leg_start").groupBy("icao24").agg(F.max("event_time").alias("_latest_start"))

    keys = (
        rows.where("NOT on_ground OR event_type = 'LANDING'")
        .select("icao24", "leg_id", "leg_start_time")
        .distinct()
    )
    if context is not None:
        # 回看窗口之前开始、还在进行中的航段：本批开了新航段，就要把它关掉，即使本批没有它的记录
        still_open = (
            context.where("status = 'IN_AIR'")
            .join(latest_start.select("icao24"), "icao24")
            .select("icao24", "leg_id", F.col("start_time").alias("leg_start_time"))
        )
        keys = keys.unionByName(still_open).distinct()
        ctx = context.select(
            "leg_id",
            F.col("last_seen").alias("_ctx_last_seen"),
            F.col("departure_kind").alias("_ctx_dep_kind"),
            F.col("departure_iata").alias("_ctx_dep_iata"),
            F.col("departure_name").alias("_ctx_dep_name"),
            F.col("departure_city").alias("_ctx_dep_city"),
        )
    else:
        ctx = None

    legs = (
        keys.join(agg, "leg_id", "left")
        .join(landings, "leg_id", "left")
        .join(latest_start, "icao24", "left")
    )
    if ctx is not None:
        legs = legs.join(ctx, "leg_id", "left")
    else:
        legs = (legs
                .withColumn("_ctx_last_seen", F.lit(None).cast("timestamp"))
                .withColumn("_ctx_dep_kind", F.lit(None).cast("string"))
                .withColumn("_ctx_dep_iata", F.lit(None).cast("string"))
                .withColumn("_ctx_dep_name", F.lit(None).cast("string"))
                .withColumn("_ctx_dep_city", F.lit(None).cast("string")))

    status = f"""
        CASE
            WHEN _land IS NOT NULL                                  THEN 'LANDED'
            WHEN COALESCE(_latest_start > leg_start_time, FALSE)    THEN 'LOST'
            WHEN unix_timestamp(last_seen)
                 < unix_timestamp(TIMESTAMP '{clock}') - {LEG_GAP_S} THEN 'LOST'
            ELSE 'IN_AIR'
        END
    """
    has_dep = F.col("_dep").isNotNull()
    landed = F.col("status") == "LANDED"
    return (
        legs
        .withColumn("last_seen", F.greatest("_agg_last_seen", "_ctx_last_seen"))
        .withColumn("status", F.expr(status))
        .select(
            "leg_id",
            "icao24",
            F.col("leg_start_time").alias("start_time"),
            "last_seen",
            F.when(landed, F.col("_land.time"))
             .when(F.col("status") == "LOST", F.col("last_seen")).alias("end_time"),
            "status",
            # 丢失原因要看合并之后的最后位置，统一在 finalize_legs 里判
            F.lit(None).cast("string").alias("lost_reason"),
            F.when(has_dep, F.col("_dep.kind")).otherwise(F.col("_ctx_dep_kind")).alias("departure_kind"),
            F.when(has_dep, F.col("_dep.iata")).otherwise(F.col("_ctx_dep_iata")).alias("departure_iata"),
            F.when(has_dep, F.col("_dep.name")).otherwise(F.col("_ctx_dep_name")).alias("departure_name"),
            F.when(has_dep, F.col("_dep.city")).otherwise(F.col("_ctx_dep_city")).alias("departure_city"),
            F.when(landed, F.col("_land.iata")).alias("arrival_iata"),
            F.when(landed, F.col("_land.name")).alias("arrival_name"),
            F.when(landed, F.col("_land.city")).alias("arrival_city"),
            F.col("_last.callsign").alias("callsign"),
            F.col("_last.latitude").alias("last_latitude"),
            F.col("_last.longitude").alias("last_longitude"),
            F.col("_last.altitude_m").alias("last_altitude_m"),
            F.col("_last.flight_phase").alias("last_flight_phase"),
            F.col("_last.near_airport_iata").alias("last_airport_iata"),
            F.col("_last.airport_dist_km").alias("last_airport_dist_km"),
            F.col("_agg_max_alt").alias("max_altitude_m"),
            F.to_date("leg_start_time").alias("dt"),
        )
    )


def _status_rank(col: str) -> str:
    return f"CASE {col} WHEN 'LANDED' THEN 2 WHEN 'LOST' THEN 1 WHEN 'IN_AIR' THEN 0 ELSE -1 END"


def apply_leg_updates(existing: DataFrame, updates: DataFrame) -> DataFrame:
    """把增量航段行合并进表里已有的行，返回合并后的整行。规则全是单调的，重复合并结果不变：

      开始时间、起飞机场：已有的为准（写入后不变）
      最后出现时间、最大高度：取较大值
      最后一条空中记录的各列（位置、呼号、飞行阶段、最近机场）：取最后出现时间较晚的一边
      状态、结束时间、落地机场：按 LANDED > LOST > IN_AIR 取高的一边，相同时取增量
      丢失原因：随状态取，最后由 finalize_legs 按合并后的行重判
    """
    e = existing.select([F.col(c).alias(f"e_{c}") for c in LEG_COLUMNS])
    u = updates.select(*LEG_COLUMNS)
    j = u.join(e, u["leg_id"] == e["e_leg_id"], "left")

    has_e = F.col("e_leg_id").isNotNull()
    take_u_status = F.expr(f"{_status_rank('status')} >= {_status_rank('e_status')}")
    take_u_pos = F.col("last_latitude").isNotNull() & (
        F.col("e_last_seen").isNull() | (F.col("last_seen") >= F.col("e_last_seen")))

    def pick(cond, col):
        return F.when(cond, F.col(col)).otherwise(F.col(f"e_{col}")).alias(col)

    return j.select(
        "leg_id",
        F.coalesce("e_icao24", "icao24").alias("icao24"),
        F.coalesce("e_start_time", "start_time").alias("start_time"),
        F.greatest("e_last_seen", "last_seen").alias("last_seen"),
        pick(take_u_status, "end_time"),
        pick(take_u_status, "status"),
        pick(take_u_status, "lost_reason"),
        pick(~has_e, "departure_kind"),
        pick(~has_e, "departure_iata"),
        pick(~has_e, "departure_name"),
        pick(~has_e, "departure_city"),
        pick(take_u_status, "arrival_iata"),
        pick(take_u_status, "arrival_name"),
        pick(take_u_status, "arrival_city"),
        pick(take_u_pos, "callsign"),
        pick(take_u_pos, "last_latitude"),
        pick(take_u_pos, "last_longitude"),
        pick(take_u_pos, "last_altitude_m"),
        pick(take_u_pos, "last_flight_phase"),
        pick(take_u_pos, "last_airport_iata"),
        pick(take_u_pos, "last_airport_dist_km"),
        F.greatest("e_max_altitude_m", "max_altitude_m").alias("max_altitude_m"),
        F.coalesce("e_dt", "dt").alias("dt"),
    )


def finalize_legs(legs: DataFrame, clock: str) -> DataFrame:
    """航段输出前的最后一步，批量和流式都经过这里：

      进行中的航段超过 20 分钟没有空中记录就记 LOST，结束时间取最后一条空中记录；
      LOST 的航段按最后一条空中记录判丢失原因，其余状态的丢失原因为空。
    """
    expired = F.expr(f"status = 'IN_AIR' AND "
                     f"unix_timestamp(last_seen) < unix_timestamp(TIMESTAMP '{clock}') - {LEG_GAP_S}")
    return (
        legs.withColumn("_expired", expired)
        .withColumn("end_time", F.when(F.col("_expired"), F.col("last_seen")).otherwise(F.col("end_time")))
        .withColumn("status", F.when(F.col("_expired"), F.lit("LOST")).otherwise(F.col("status")))
        .withColumn("lost_reason", F.expr(LOST_REASON))
        .drop("_expired")
        .select(*LEG_COLUMNS)
    )


# ================================================================
#  两种用法：批量重算、流式单批
# ================================================================
# ★ 为什么要传 cache
#   下面几个中间结果会被后面的步骤用好几次（比如带窗口函数的 assigned 要汇总、取落地、取开始
#   时间、取 key 四次）。不物化的话，执行时每用一次就重算一次；优化器估算 join 大小时还会把
#   各个分支的大小连乘，计划一深，估算值大到 BigInteger 要算很久，单测里曾因此内存溢出。
#   作业传入"persist 并立刻 count"（dwd_event_leg.Materializer）；单测数据小，传入"收集后重建
#   本地 DataFrame"；默认不物化，方便读代码。
def _no_cache(df: DataFrame) -> DataFrame:
    return df


def compute_all(states: DataFrame, clock: str = None, cache=_no_cache):
    """批量重算：全部状态明细 → (起降事件, 航段)。clock 默认取数据里最晚的快照时间。"""
    require_utc(states)
    seq = states.select(*STATE_COLUMNS).withColumn("_decide", F.lit(True))
    if clock is None:
        clock = seq.agg(F.date_format(F.max("event_time"), TS_FMT)).first()[0] or "1970-01-01 00:00:00"
    flipped = cache(with_flips(seq))
    assigned = cache(with_legs(mark_leg_starts(flipped)))
    legs = leg_updates(assigned, None, clock)
    return events_from(flipped), finalize_legs(legs, clock)


def latest_legs_before(legs: DataFrame, firsts: DataFrame, lookback_min: str) -> DataFrame:
    """每架飞机在本批第一条记录之前开始、最近 20 分钟内还有空中记录的最后一个航段。

    回看窗口里最后一条空中记录属于哪个航段，就是它。lookback_min 只用来让 Delta 跳过旧文件。
    """
    w = Window.partitionBy("icao24").orderBy(F.col("start_time").desc())
    return (
        legs.where(f"last_seen >= TIMESTAMP '{lookback_min}'")
        .join(firsts, "icao24")
        .where(f"start_time < _first AND "
               f"unix_timestamp(last_seen) >= unix_timestamp(_first) - {LEG_GAP_S}")
        .withColumn("_rn", F.row_number().over(w))
        .where("_rn = 1")
        .select(*LEG_COLUMNS)
    )


def stream_step(batch: DataFrame, states: DataFrame, legs: DataFrame, cache=_no_cache):
    """流式一个微批的计算，不读写 Delta。

      batch   本批新增的状态明细
      states  状态明细全表（可以已经包含本批），只读本批飞机回看窗口里的记录
      legs    航段表当前的内容
      cache   物化中间结果的函数，见上面的说明

    返回 (events, leg_rows, bounds)。events 按主键只插入；leg_rows 是要按 leg_id 整行
    upsert 的航段；bounds 是本批的时间范围（n、t_min、t_max、lookback_min，UTC 字符串）。
    本批为空时返回 None。
    """
    require_utc(batch)
    batch = batch.select(*STATE_COLUMNS)
    legs = legs.select(*LEG_COLUMNS)
    b = batch.agg(
        F.count(F.lit(1)).alias("n"),
        F.date_format(F.min("event_time"), TS_FMT).alias("t_min"),
        F.date_format(F.max("event_time"), TS_FMT).alias("t_max"),
        F.date_format(F.min("event_time") - F.expr(f"INTERVAL {LEG_GAP_S} SECONDS"), TS_FMT)
         .alias("lookback_min"),
    ).first()
    if b["n"] == 0:
        return None
    clock = b["t_max"]
    firsts = cache(batch.groupBy("icao24").agg(F.min("event_time").alias("_first")))

    # 回看：本批每架飞机在各自第一条记录之前 20 分钟内的记录。
    # 先用常量时间范围（Delta 据此跳过无关文件），再按每架飞机精确过滤
    lookback = (
        states
        .where(f"dt BETWEEN DATE '{b['lookback_min'][:10]}' AND DATE '{b['t_max'][:10]}'")
        .where(f"event_time >= TIMESTAMP '{b['lookback_min']}' AND event_time < TIMESTAMP '{b['t_max']}'")
        .select(*STATE_COLUMNS)
        .join(firsts, "icao24")
        .where(f"event_time < _first AND unix_timestamp(event_time) >= unix_timestamp(_first) - {LEG_GAP_S}")
        .drop("_first")
    )
    context = cache(latest_legs_before(legs, firsts, b["lookback_min"]))

    seq = (batch.withColumn("_decide", F.lit(True))
           .unionByName(lookback.withColumn("_decide", F.lit(False))))
    flipped = cache(with_flips(seq))
    events = events_from(flipped.where("_decide"))
    assigned = cache(with_legs(mark_leg_starts(flipped), context))
    updates = cache(leg_updates(assigned, context, clock))

    # 本批涉及的航段和表里已有的行合并；表里其他超时的进行中航段一起关掉
    existing = (legs.where(f"last_seen >= TIMESTAMP '{b['lookback_min']}'")
                .join(updates.select("leg_id"), "leg_id", "left_semi"))
    merged = cache(apply_leg_updates(existing, updates))
    stale = (
        legs.where(f"status = 'IN_AIR' AND "
                   f"unix_timestamp(last_seen) < unix_timestamp(TIMESTAMP '{clock}') - {LEG_GAP_S}")
        .join(merged.select("leg_id"), "leg_id", "left_anti")
    )
    leg_rows = finalize_legs(merged.unionByName(stale), clock)
    return events, leg_rows, b


def require_utc(df: DataFrame) -> None:
    tz = df.sparkSession.conf.get("spark.sql.session.timeZone")
    if tz not in ("UTC", "Etc/UTC", "GMT", "Z", "+00:00"):
        raise ValueError(
            f"dt 按 UTC 日期分区，Spark 会话时区必须是 UTC，现在是 {tz}。"
            "在 SparkSession 上设置 spark.sql.session.timeZone=UTC。"
        )
