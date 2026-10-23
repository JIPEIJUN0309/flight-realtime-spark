# 架构与数据流

> 指标口径、表清单和 SLA 以 [requirements.md](requirements.md) 为准，这里只讲组件怎么配合、数据怎么流。
> 运维命令见 [runbook.md](runbook.md)。

![表间关系：起降事件和航段都从状态明细推出](images/table-lineage.png)

## 组件

| 组件 | 作用 | 代码或配置 |
| --- | --- | --- |
| 采集器 | 每 10 秒轮询 OpenSky，写入 Kafka | `ingestion/opensky_producer.py` |
| Kafka（KRaft） | 缓冲与回放，单分区，消息 key 是 icao24 | `docker-compose.yml` |
| Spark Structured Streaming | 实时作业：一个流读 Kafka，每 5 秒一批，写 ODS、状态明细、起降事件、航段、最新状态，并在同一个微批里同步 PostgreSQL | `spark/rt_pipeline.py` |
| Spark 批作业 | DWS 汇总、ADS 导出、异常检测打分 | `spark/dws_batch.py`、`spark/ads_export.py`、`spark/ml_score.py` |
| Delta Lake | ODS / DWD / DWS / ADS 分层存储 | Docker 卷 `lake_delta`（容器里 `/data/delta`），checkpoint 在卷 `lake_checkpoint` |
| Airflow | 每 15 分钟调度批链路，按数据区间计算，可用 `dags backfill` 补数 | `airflow/dags/flight_batch_pipeline.py` |
| PostgreSQL | 服务层：实时三张表每批一个事务 upsert；统计表由 ADS 整表覆盖 | `spark/pg_sync.py`、`spark/ads_export.py` |
| Grafana | 实时卡片（5 秒刷新）和统计报表（1 分钟刷新），两块看板都读 PostgreSQL | `grafana/`、`docker-compose.yml` |

## 数据流

### 实时链路（常驻）

```
OpenSky REST ──10s──▶ Kafka(flights) ──5s 微批──▶ bronze_flights（追加）
                                        │
                                        ├──▶ dwd_flight_state（按主键只插入，含地面记录）
                                        │        ├──▶ dwd_airport_event（起降事件，只插入）
                                        │        └──▶ dwd_flight_leg（航段累积快照，MERGE）
                                        └──▶ ads_latest_position（每架飞机最新状态，MERGE）
                                                 │
                                                 └──▶ PostgreSQL 三张表（一个事务）
```

- 一个流查询读 Kafka 一次，`foreachBatch` 里做完上面全部写入。Kafka 只读一遍，
  各表靠各自的 checkpoint 保证重放幂等。
- 触发是 5 秒一次的处理时间触发；采集是 10 秒一轮，所以大约一半的批是空的。
- 服务层和湖在同一个微批里写，PostgreSQL 先提交：PG 写失败就整批失败、从 checkpoint 重放，
  湖里不会出现 PG 没有的数据；湖是事实来源，作业每次启动先按湖全量对齐 PG。
- 数据时钟（已处理数据里最晚的快照时间）决定"当前""已离开""航段超时"，不看机器时间。

### 批链路（每 15 分钟）

```
dwd_flight_state ──┐
                   ├─▶ dws_batch.py ──▶ dws_airport_5min / dws_airport_hourly
dwd_airport_event ─┘                   gold_country_5min / gold_overview_5min
                                              │
                                              ▼
                                        ads_export.py ──▶ 5 张 ADS 表 ──▶ PostgreSQL
                                              │
                                        ml_features.py ──▶ Parquet ──▶ ml_score.py ──▶ ads_ml_anomaly
```

- Airflow 的每个任务带 `--start/--end`（本次数据区间，往前多看 1 小时），只重算这段区间；
  写表用 `replaceWhere`（DWS）和带区间条件的 MERGE（ADS），区间外一行不动。
- `airflow dags backfill` 会给每个历史区间各生成一次运行，所以补数和日常运行走同一条代码路径。
- 训练（`ml_train.py` / `ml_eval.py`）不进调度：换模型要人看 sweep 和 PR-AUC 后决定。

## 表清单

| 层 | 表 | 路径（容器内） | 分区 | 主键 | 写入方式 | 刷新 |
| --- | --- | --- | --- | --- | --- | --- |
| ODS | bronze_flights | `/data/delta/bronze_flights` | 无（平铺） | (partition, offset) 语义 | 流式追加 | 5 秒 |
| DWD | dwd_flight_state | `/data/delta/dwd_flight_state` | `dt`（UTC 日期） | (icao24, event_time) | 流式 MERGE 只插入；`backfill` 按区间 `replaceWhere` | 5 秒 |
| DWD | dwd_airport_event | `/data/delta/dwd_airport_event` | `dt` | (icao24, event_time, event_type) | 流式只插入 | 5 秒 |
| DWD | dwd_flight_leg | `/data/delta/dwd_flight_leg` | `dt` | leg_id | 每批按主键整段替换（`replace_rows`），累积快照 | 5 秒 |
| DWS | dws_airport_5min | `/data/delta/dws_airport_5min` | `dt` | (window_start, airport_iata) | 批处理 `replaceWhere` | 15 分钟 |
| DWS | dws_airport_hourly | `/data/delta/dws_airport_hourly` | `dt` | (hour_start, airport_iata) | 批处理 `replaceWhere` | 15 分钟 |
| DWS | gold_country_5min | `/data/delta/gold_country_5min` | `dt` | (window_start, origin_country) | 批处理 `replaceWhere` | 15 分钟 |
| DWS | gold_overview_5min | `/data/delta/gold_overview_5min` | `dt` | window_start | 批处理 `replaceWhere` | 15 分钟 |
| DWS | gold_overview_stream | `/data/delta/gold_overview_stream` | `dt` | window_start | 可选流式作业 `dws_gold_stream.py`，当前不服务看板 | 30 秒 |
| ADS | ads_latest_position | `/data/delta/ads/latest_position` | 无 | icao24 | 每批按主键整段替换（`replace_rows`） | 5 秒 |
| ADS | ads_airport_traffic 等 5 张 | `/data/delta/ads/*` | `dt` | 窗口 + 维度 | 批处理 MERGE（删除只限重算区间） | 15 分钟 |
| 服务层 | PG 实时三张表 | PostgreSQL | — | 同上游 | 微批内一个事务 upsert | 5 秒 |
| 服务层 | PG 统计表 | PostgreSQL | — | 同上游 | 整表覆盖（见已知限制） | 15 分钟 |

> `silver_flights` 是 M4 之前的过渡表（状态明细的空中子集），代码已下线，只剩
> 引擎对比的基准脚本在用它造数据；不再随实时链路更新。

## 容量推演

实测（2026-10-21，10 秒轮询，累计约 122 万条 Kafka 消息）：

| 项 | 实测 | 折算每天（10 秒轮询） |
| --- | --- | --- |
| bronze | 112 MB / 122 万条 ≈ 93 B/条 | 8,640 轮 × 155 架 ≈ 134 万条 ≈ 125 MB |
| dwd_flight_state | 96 MB / 约 80 万条 ≈ 120 B/条 | ≈ 160 MB |
| dwd_flight_leg | 121 MB（累积快照，每批更新） | 增长主要来自新增航段，约 6–8 千/天 |
| ads（含最新状态） | 185 MB | 最新状态表按飞机数封顶 |

按 30 秒轮询（日常省额度）三项都降到 1/3。

放大 100 倍（全球范围或更密的采集）：

- 日增约 1.3 亿条、12 GB 的 bronze。按 5 秒一个微批写，一天 17,280 个文件，
  必须按小时分区并定时 `OPTIMIZE`（小文件合并），否则 Delta 列目录就会变慢。
- 状态明细单表每天 1.6 亿行：分区键要从 `dt` 细化到 `dt + hour`，并按 icao24 分桶，
  否则 MERGE 的条件虽然带时间范围，仍会扫到过大的分区。
- DWS 的规模不随飞机数线性涨：5 分钟窗口 × 20 个机场 = 5,760 行/天，主要成本在扫描明细。
- 批链路要改成按区间并行（Airflow 动态任务映射），否则 15 分钟跑不完 15 分钟的增量。
