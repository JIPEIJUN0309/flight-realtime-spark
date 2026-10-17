# 架构与数据流

> 骨架，待补充。指标口径、表清单和 SLA 以 [requirements.md](requirements.md) 为准，这里只讲组件怎么配合、数据怎么流。

![表间关系：起降事件和航段都从状态明细推出](images/table-lineage.png)

## 组件

| 组件 | 作用 | 代码或配置 |
| --- | --- | --- |
| 采集器 | 轮询 OpenSky，写入 Kafka | `ingestion/` |
| Kafka（KRaft） | 缓冲与回放 | `docker-compose.yml` |
| Spark Structured Streaming | ODS 入湖；规划中：状态明细、起降事件、航段、最新状态的流式更新 | `spark/` |
| Spark 批作业 | DWS 汇总、告警、异常检测打分 | `spark/` |
| Delta Lake | ODS / DWD / DWS / ADS 分层存储 | 数据目录下的 `delta/`（`.env` 的 `DATA_DIR`，放在仓库外） |
| Airflow | 批链路调度与补数 | `airflow/` |
| PostgreSQL | 服务层 | `docker-compose.yml` |
| Grafana / Metabase | 实时卡片 / 统计报表 | Grafana 待接入；Metabase 在 `docker-compose.yml` |

## 部署

三套 Docker Compose：

- 主栈 `docker-compose.yml`：kafka、kafka-ui（8090）、spark（Spark UI 4040）、postgres、flight-metabase（3001）
- Airflow `docker-compose.airflow.yml`：Web UI 8081
- Hive `docker-compose.hive.yml`：只用于 Hive 与 Spark 的引擎对比

## 待补

- [ ] 实时链路和批链路各自的触发方式与数据流向
- [ ] 每张表的存储路径和分区方式
- [ ] 容量推演：数据量放大 100 倍时，分区数、并行度、状态大小、存储分别怎么变
