# 运维手册

> 每个场景按"现象 → 定位 → 恢复 → 验证"记录。场景来自 [requirements.md](requirements.md) 的验收方式，演练后填入真实输出和耗时。

## 日常操作

- 启动主栈：`docker compose start`，Metabase 在 http://localhost:3001
- 启动采集器：`python ingestion/opensky_producer.py`
- 启动入湖流作业：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/ods_bronze.py`
- 收工：Ctrl+C 停掉采集器和流作业，再 `docker compose stop`

## 故障场景

### 1. 流作业中途被终止

- 现象：
- 定位：
- 恢复：用同一个 checkpoint 重启 `ods_bronze.py`
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
- 恢复：
- 验证标准：记录 Delta 并发冲突的现象，以及错开分区或暂停流之后的结果
- 演练记录：待演练

### 6. 迟到数据超过水位线

- 现象：
- 定位：
- 恢复：
- 验证标准：查看 numRowsDroppedByWatermark，确认批链路重算后口径一致
- 演练记录：待演练
