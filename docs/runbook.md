# 运维手册

> 每个场景按"现象 → 定位 → 恢复 → 验证"记录。场景来自 [requirements.md](requirements.md) 的验收方式，演练后填入真实输出和耗时。

## 日常操作

- 数据目录：`.env` 里的 `DATA_DIR`（例如 `D:/data/flight-realtime-spark`），容器里是 `/data`。没设置时 `docker compose` 会直接报错
- 启动主栈：`docker compose start`，Metabase 在 http://localhost:3001
- 启动采集器：`python ingestion/opensky_producer.py`
- 启动入湖流作业：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/ods_bronze.py`
- 启动状态明细流作业（另开一个终端）：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dwd_state.py`
- 收工：Ctrl+C 停掉采集器和流作业，再 `docker compose stop`

### 状态明细的对账与回刷

时间按 UTC 写，区间左闭右开。

- 对账：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dwd_state.py verify --start "2026-10-17 08:00" --end "2026-10-17 09:00"`。输出“完全一致”、退出码为 0 才算通过；区间要选流作业已经处理完的时段
- 回刷：把 `verify` 换成 `backfill`，只覆盖这个区间。清洗规则改了以后，先重启流作业，再回刷规则生效前的区间
- 只追平积压、不常驻：`dwd_state.py stream --available-now`
- 重建表：`docker exec spark rm -rf /data/delta/dwd_flight_state /data/checkpoint/dwd_flight_state`，表和 checkpoint 一起删。只删表不删 checkpoint，流作业会从上次读到的位置继续，之前的数据不会自动重算（可以用 backfill 补）

## 故障场景

### 1. 流作业中途被终止

- 现象：
- 定位：
- 恢复：用同一个 checkpoint 重启 `ods_bronze.py`；`dwd_state.py` 同样直接重启，重放的那一批如果之前已经写进表，日志会显示“新插入 0 条”
- 验证标准：运行 `sql/checks/exactly_once_offsets.sql`，演练时间段内 (partition, offset) 无重复、无断号
- 演练记录：待演练

### 2. 采集中断（断网或 429 限流）

- 现象：
- 定位：
- 恢复：
- 验证标准：约 30 秒时收到 P1 告警，恢复后告警自动关闭，数据自动续上
- 演练记录：待演练

### 3. 批任务失败后重跑

- 现象：
- 定位：
- 恢复：
- 验证标准：同一区间连续跑两次，行数和校验和一致
- 演练记录：待演练

### 4. PostgreSQL 宕机

- 现象：
- 定位：
- 恢复：
- 验证标准：发布失败但湖内数据不受影响，恢复后能补发
- 演练记录：待演练

### 5. 流式 MERGE 与回刷同时写一张表

- 现象：
- 定位：
- 恢复：`dwd_state.py` 的 MERGE 遇到 Delta 并发冲突会自动重试 3 次；回刷失败直接重跑，它按区间覆盖，重跑结果一样
- 验证标准：记录 Delta 并发冲突的现象，以及错开分区或暂停流之后的结果
- 演练记录：待演练

### 6. 迟到数据超过水位线

- 现象：
- 定位：
- 恢复：
- 验证标准：查看 numRowsDroppedByWatermark，确认批链路重算后口径一致
- 演练记录：待演练

## 验收记录

| 日期 | 里程碑 | 验证 | 结果 |
| --- | --- | --- | --- |
| 2026-10-17 | M1 | 清洗规则单测，在 spark 容器里跑 | 35 个全部通过 |
| 2026-10-17 | M1 | 首次流式写入 dwd_flight_state（`stream --available-now`，7 月全部历史） | 一批读入 177,584 条，过滤和去重 87 条（0.05%），新插入 177,497 条，耗时 12.0 秒 |
| 2026-10-17 | M1 | 流批对账（`verify`，2026-01-01 至 2026-10-18） | 表内 177,497 行，与批量重算完全一致；只在一边的行 0，主键重复 0 |
| 2026-10-17 | M1 | 删掉 checkpoint 后全部重放 | 新插入 0 条（177,497 条表里已有），耗时 8.9 秒 |
