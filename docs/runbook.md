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

- 对账：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dwd_state.py verify --start "2026-10-17 08:00" --end "2026-10-17 09:00"`。输出“完全一致”、退出码为 0 才算通过；区间要选流作业已经处理完、两边都有数据的时段，区间里没有数据会报“不能算通过”
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
| 2026-10-17 | M1 | 实时流批对账（10 秒轮询，`verify` 08:50–09:00 UTC） | bronze 原始 53,763 条，表内 53,763 行，与批量重算完全一致；只在一边的行 0，主键重复 0，这段没有被过滤或去重的记录 |
| 2026-10-17 | M1 | 同一次实时运行的采集与微批耗时（M3 优化前的对比基线） | 采集器 10 秒一轮，没有发送失败；ODS 每批 6–7 秒，dwd_state 每次触发 7–9 秒，都超过 5 秒的触发间隔，不丢数据，实时延迟留到 M3 优化 |
| 2026-10-17 | M0 | ZRH 地面覆盖（`sql/checks/zrh_ground_coverage.sql`，7 月历史加今天的实时数据） | ZRH 5 km 内地面记录 26,872 条、303 架，空中 1,617 条、297 架；按起降规则 5 个采集日判出落地 204 次、起飞 187 次，另有 43 次翻转因前后两条间隔超过 1 分钟不计（约 10%，30 秒和 10 秒轮询下比例相近）。地面覆盖足够，M2 按 on_ground 翻转判定起降 |
| 2026-10-17 | M0 | 改造前延迟基线（采集 10 秒一轮时，手动跑一遍批链路 dwd_silver → dws_gold_batch → ads_export） | 跑完时 PostgreSQL 的 ads_latest_position 新鲜度 145 秒，其中 ads_export 68 秒。改造前看板靠 Airflow 每 15 分钟跑一次这条链路刷新，按此推算新鲜度在 145 秒到约 17 分钟之间，P95 约 1,000 秒，处理延迟 P95 约 16 分钟；M3 的目标分别是 30 秒和 10 秒 |
