# 运维手册

> 每个场景按"现象 → 定位 → 恢复 → 验证"记录。场景来自 [requirements.md](requirements.md) 的验收方式，演练后填入真实输出和耗时。

## 日常操作

- 数据目录：`.env` 里的 `DATA_DIR`（例如 `D:/data/flight-realtime-spark`），容器里是 `/data`。没设置时 `docker compose` 会直接报错。其中 Delta 表和 checkpoint 在 Docker 卷里，见下面“数据目录与 Docker 卷”
- 启动主栈：`docker compose start`。Metabase（统计报表）在 http://localhost:3001，Grafana（实时卡片，免登录只读）在 http://localhost:3002
- 启动采集器：`python ingestion/opensky_producer.py`
- 启动实时作业（M3 起一个作业写完 ODS、状态明细、起降事件、航段、最新状态和 PostgreSQL）：`docker exec -it spark /opt/spark/bin/spark-submit --driver-memory 2g /opt/spark/work-dir/jobs/rt_pipeline.py`
- 不要再同时开 `ods_bronze.py`、`dwd_state.py`、`dwd_event_leg.py` 的常驻流：它们会重复写同几张表（`ods_bronze.py` 是直接追加，会让同一条消息进 bronze 两次）
- 收工：Ctrl+C 停掉采集器和实时作业，再 `docker compose stop`。不演示时把 `.env` 的 `POLL_INTERVAL` 改回 30

### 数据目录与 Docker 卷

- `delta/` 和 `checkpoint/` 在 Docker 卷 `flight-realtime-spark_lake_delta`、`flight-realtime-spark_lake_checkpoint` 里，挂在 spark 容器的 `/data/delta`、`/data/checkpoint`；`bench/`、`ml/` 等仍在 `DATA_DIR`。原因：Windows 的盘经 9p 挂进容器，小文件元数据操作慢一两千倍，Delta 每次读写都要列一遍 `_delta_log`（M3 诊断：bronze 1,025 个日志文件，列一次 7.5 秒）
- 看文件：`docker exec spark ls /data/delta`；拷出来：`docker cp spark:/data/delta/<表> <本机目录>`
- 不要用 `docker compose down -v`，它会连卷一起删。D 盘 `DATA_DIR` 下旧的 `delta/`、`checkpoint/` 是 2026-10-20 迁移前的备份，容器里已经看不到它们
- 迁移（只需一次，2026-10-20 已做过：delta 126 MB、checkpoint 6.8 MB）：`docker compose up -d spark` 重建容器并建好空卷，再 `docker compose run --rm --no-deps -v D:/data/flight-realtime-spark:/old spark bash -c "cp -a /old/delta/. /data/delta/ && cp -a /old/checkpoint/. /data/checkpoint/"`。`docker compose run` 会接管窗口的输入，这条要单独粘贴，否则后面一起粘贴的命令会被它吞掉

### 实时链路（rt_pipeline.py）

一个流作业读 Kafka，每 5 秒一批：清洗 → 判起降、切航段、算最新状态 → 同时写服务层（一个 PostgreSQL 事务）和湖（起降事件、航段、最新状态并发提交），状态明细最后提交。bronze 在一批开头就在后台读 Kafka、写表，最新状态也在后台算，和判起降、切航段同时进行。每批一行日志：读入条数、新状态是否要和表反连接去重（正常运行时每批都是新快照，显示“跳过反连接”）、各段耗时、处理延迟；“计算”后面括号里是计算的各步（清洗/新行/航段/等最新，以及后台算最新状态用了多久），慢了先看这里。

- 启动时先做三件事：检查表结构；最新状态表不存在就从状态明细全量建；PostgreSQL 建表并按湖全量对齐。第一次启动（还没有 checkpoint）从 bronze 里最大的 offset 之后读 Kafka，从旧的 `ods_bronze.py` 切过来不重读、不漏读
- 最新状态表结构和代码不一致（例如批链路以前写的旧表）时会退出并提示：`docker exec spark rm -rf /data/delta/ads/latest_position`，再启动会从状态明细重建
- 只追平积压、不常驻：`rt_pipeline.py stream --available-now`
- 启动时把状态明细、起降事件、航段、最新状态四张表的 checkpoint 间隔设成每 50 次提交一次（Delta 默认 10 次，环境变量 `RT_DELTA_CHECKPOINT_INTERVAL` 可改）。默认间隔下，每 10 批就有一批要在提交时同步写 checkpoint，那张表多花 0.6–0.9 秒，批耗时会贴近 5 秒
- 输出想同时存成文件（事后看每批耗时、出错时看报错）：`docker exec -it spark bash -c "/opt/spark/bin/spark-submit --driver-memory 2g /opt/spark/work-dir/jobs/rt_pipeline.py 2>&1 | tee -i /opt/spark/work-dir/jobs/rt.log"`，文件在仓库的 `spark\rt.log`（`*.log` 被 git 忽略）。`tee -i` 不理 Ctrl+C，停作业时日志能写完整
- 停止：在作业窗口按 Ctrl+C，或者 `docker exec spark pkill -f rt_pipeline.py`。作业先提示“等正在写的这一批写完再停”，写完后停掉查询、打印“已停止”再退出，不会停在一批写到一半的地方；急着停就再按一次 Ctrl+C，立即退出，没写完的那一批重启后从 checkpoint 整批重放（各表写入都幂等）。没加 `-it` 时在窗口里按 Ctrl+C 或关掉窗口，停的只是 docker 命令，容器里的作业还在跑，要用 pkill
- 同一时间只能开一个实时作业：启动时发现容器里已有一个就退出，并提示上面的停止命令
- 对账：先停采集器，等作业不再出新批次，Ctrl+C 停掉作业，再 `docker exec spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/rt_pipeline.py verify --start "2026-10-20 08:00" --end "2026-10-20 10:00"`（UTC）。依次比状态明细、起降事件与航段（按区间）、最新状态（全表）、PostgreSQL 与湖（全表），四项都“通过”、退出码为 0 才算通过
- Grafana 地图底图用 Esri 的深灰底图（`services.arcgisonline.com`，不需要 key）。Grafana 默认的 CARTO 底图从 2026-08 起不带 API key 只返回“API KEY REQUIRED”水印。改了看板文件后要在浏览器里按 F5 重新加载页面，右上角的 Refresh 只重新查数据；Esri 底图也打不开时，在面板编辑里把底图换成 Open Street Map
- 批耗时和延迟记在 PostgreSQL：`rt_batch_log`（每批，Spark 统计的整批耗时和各段耗时）、`rt_sync_log`（每次同步，处理延迟）；Grafana 下方“链路监控”就是画的这两张表
- PostgreSQL 停了：作业写服务层失败就停下（Kafka 里的数据还在）。PostgreSQL 恢复后重启作业，从 checkpoint 重放失败的那一批，启动时也会按湖全量对齐
- Kafka 的 `kafka_ts` 用 broker 写入时间，和 PostgreSQL 同一个时钟，处理延迟不受 Windows 和 Docker 虚拟机时钟偏差影响。一次性设置：`docker exec kafka kafka-configs --bootstrap-server localhost:9092 --alter --entity-type topics --entity-name flights --add-config message.timestamp.type=LogAppendTime`
- 验收采样：`sql/checks/m3_sla_sampler.sql`（每 10 秒采新鲜度和处理延迟，每 0.2 秒查一次服务层有没有半批），汇总用 `sql/checks/m3_sla_report.sql`，跑法写在文件开头

### 状态明细的对账与回刷

时间按 UTC 写，区间左闭右开。

- 对账：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dwd_state.py verify --start "2026-10-17 08:00" --end "2026-10-17 09:00"`。输出“完全一致”、退出码为 0 才算通过；区间要选流作业已经处理完、两边都有数据的时段，区间里没有数据会报“不能算通过”
- 回刷：把 `verify` 换成 `backfill`，只覆盖这个区间。清洗规则改了以后，先重启流作业，再回刷规则生效前的区间
- 只追平积压、不常驻：`dwd_state.py stream --available-now`（M3 起状态明细由实时作业写，平时用不到；不要和实时作业同时跑）
- 重建表：`docker exec spark rm -rf /data/delta/dwd_flight_state /data/checkpoint/dwd_flight_state`，表和 checkpoint 一起删。只删表不删 checkpoint，流作业会从上次读到的位置继续，之前的数据不会自动重算（可以用 backfill 补）
- 状态明细回刷过之后，起降事件和航段不会自动跟着改，要对它们 rebuild（见下一节）

### 起降事件与航段

dwd_airport_event 和 dwd_flight_leg 在 M2 由流作业 `dwd_event_leg.py` 写入（读状态明细的新增行），M3 起由实时作业 `rt_pipeline.py` 在同一个微批里写，规则还是 `spark/flight_legs.py` 那一份。`dwd_event_leg.py` 的 rebuild、verify 照常用；它的 stream 不要和实时作业同时跑。

- 第一次启动：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dwd_event_leg.py stream --available-now`，把状态明细里已有的数据在一批里处理完再退出，然后再常驻启动
- 对账：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dwd_event_leg.py verify --start "2026-10-19 08:00" --end "2026-10-19 09:00"`。先停掉采集器，等 dwd_state 和本作业都不再出新批次再跑：航段是否超时（LOST）取决于最新的数据时间，两边处理到的位置要一样。起降事件按事件时间、航段按开始时间取区间
- 重建：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dwd_event_leg.py rebuild`，从状态明细全量重算并覆盖两张表。规则改了、或状态明细回刷过之后用。先停掉本作业，重建完直接重启，checkpoint 不用删，重复处理的部分结果不变
- 表结构变了（代码给表加了列）：先停掉本作业，`docker exec spark rm -rf /data/delta/dwd_airport_event /data/delta/dwd_flight_leg` 删掉两张表，再 rebuild，checkpoint 不用删。stream、rebuild、verify 启动时发现表结构和代码不一致，会直接退出并打印这条命令。不自动改表：Delta 的 CREATE OR REPLACE 对按路径建的表不换表结构，会报 DELTA_CREATE_TABLE_SCHEME_MISMATCH
- 改了采集器的 `BBOX_*`：判断航段“飞出范围”要用当时的采集范围，在 `spark/flight_legs.py` 的 `COVERAGE` 加一行生效时间（UTC），同步需求文档，再 rebuild；用 `sql/checks/coverage_history.sql` 核对
- 从头再来：`docker exec spark rm -rf /data/delta/dwd_airport_event /data/delta/dwd_flight_leg /data/checkpoint/dwd_event_leg`，两张表和 checkpoint 一起删，再按"第一次启动"跑
- 落地抽查：`sql/checks/landing_spot_check.sql`，跑法见 `sql/README.md`

### 批链路（DWS / ADS）

M4 起批链路是 `dws_batch.py` → `ads_export.py` → `ml_features.py` → `ml_score.py`。
`dwd_silver.py` 和 `dws_gold_batch.py` 已删除：状态明细由实时作业写，批链路直接读它，
过渡期的 `silver_flights` 没有下游了（表还在，不再更新，只有引擎对比的基准脚本在用）。

时间按 UTC 写，区间左闭右开；区间会向下对齐到窗口边界（5 分钟表对齐 5 分钟、小时表对齐整点），
所以给 `12:07` 会从 `12:05` 算起。同一区间跑几次结果都一样。

- 日常增量（和 Airflow 每 15 分钟跑的是同一条）：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dws_batch.py`。不传区间时自己找：从表里最晚的窗口往回看 1 小时（小时表 6 小时），算到数据时钟向下取整的那个边界
- 指定区间回刷：`... dws_batch.py backfill --start "2026-10-20 00:00" --end "2026-10-20 06:00"`。先确认这段的状态明细和起降事件已经写完（实时作业追平），否则算出来的就是缺数据的版本
- 对账：`... dws_batch.py verify --start ... --end ...`，四张表逐表比"表里"和"现在重算"，两个方向的差都是 0、退出码 0 才算通过
- ADS：`... ads_export.py`（默认跟 DWS 的最新窗口）、`... ads_export.py --start ... --end ...`（和 DWS 用同一区间）
- 表结构变了（M4 把 `flight_cnt` 改成 `aircraft_cnt` 就是这种情况）：DWS 和 ADS 都是派生数据，任务启动时发现列对不上会**自动整表重建**并打印一行日志，不用手工删表。但**带 `--start/--end` 的补数**遇到结构不一致会直接报错退出——按区间覆盖会把区间外的历史冲掉，正确做法是先不带区间跑一次让整表重建
- 重建某张 DWS 表：`docker exec spark rm -rf /data/delta/dws_airport_5min`，再不带区间跑一次 `dws_batch.py`
- 下游看板字段：ADS 和 PostgreSQL 里的列名跟着改成 `aircraft_cnt`；Metabase 里引用了旧列名的卡片要手工改（Grafana 实时卡片不读这些表，不受影响）

Airflow：

- 调度是 `*/15 * * * *`，每个任务带本次的 `data_interval_start/end`，并往前多看 1 小时
- 补数：`docker exec airflow-scheduler airflow dags backfill flight_batch_pipeline --start-date 2026-10-20 --end-date 2026-10-21`（容器名以 `docker-compose.airflow.yml` 为准），每个区间单独跑一次，互不影响
- 同一时间只跑一条链（`max_active_runs=1`），失败重试 1 次、间隔 2 分钟

### 质量门禁与告警

质量检查（`dq_checks.py`）放在 DWS 之后、ADS 之前，强规则失败时任务退出码 1，Airflow 停在这一步，这一段的 ADS 不发布（看板保留上一版）。

- 手动跑一段：`docker exec -it spark /opt/spark/bin/spark-submit /opt/spark/work-dir/jobs/dq_checks.py --start "2026-10-20 00:00" --end "2026-10-20 06:00"`。不加 `--fail-on-strong` 时只记录、不失败，排查用这个
- 结果：Delta 的 `/data/delta/dq_result`（按天分区，保留历史），PostgreSQL 的 `dq_result`（每次覆盖，给告警引擎和看板查）
- 采集频率改了要同步 `DQ_POLL_INTERVAL`（默认 10 秒），否则完整性规则的分母是错的；采集范围改了同步 `BBOX_*`
- 某条弱规则一直不过：先看 `detail` 里的数字，再对照 `docs/requirements.md` 的阈值；调阈值属于改口径，先改文档

告警引擎（`alert_engine.py`）是一个常驻的 Python 进程，每 15 秒查一次 PostgreSQL：

```
docker exec -d spark python3 /opt/spark/work-dir/jobs/alert_engine.py
docker exec spark python3 /opt/spark/work-dir/jobs/alert_engine.py --once   # 只跑一轮，排查用
docker exec spark pkill -f alert_engine.py                                  # 停止
```

- 推送：`docker-compose.yml` 里给 spark 服务加 `ALERT_WEBHOOK_URL`（飞书/钉钉机器人地址）和 `ALERT_WEBHOOK_KIND`（`feishu` / `dingtalk`），不配就只写库和日志
- 告警记录：`SELECT * FROM alert_event ORDER BY fired_at DESC LIMIT 20`；未恢复的告警 `cleared_at IS NULL`
- 规则和阈值见 `docs/requirements.md`「告警规则」，代码里的常量在 `spark/alert_engine.py` 顶部
- 演练采集中断：停掉采集器约 40 秒（`Ctrl+C` 或 `Stop-Process`），引擎在 30 秒后写一条 P1「采集中断」；恢复采集后下一轮自动写 `cleared_at`

## 故障场景

### 1. 流作业中途被终止

- 现象：进程被强制杀掉（SIGKILL / 容器 OOM），日志停在某一批中间，之后不再有输出
- 定位：`docker exec spark ps -eo pid,args | grep [r]t_pipeline` 看进程在不在；`docker exec spark cat /sys/fs/cgroup/memory.events` 看 `oom_kill` 是否增加；`docker inspect spark --format '{{.State.OOMKilled}}'`
- 恢复：用同一个 checkpoint 重启（不删 checkpoint）。重放的那一批如果之前已经写进表，日志会显示“新插入 0 条”
- 验证标准：`sql/checks/exactly_once_offsets.sql` 演练时间段内 (partition, offset) 无重复、无断号
- 演练记录：2026-10-21 07:34:42 UTC 对实时作业发 SIGKILL，07:35:20 用同一个 checkpoint 重启。从 07:34:00 起 partition 0 的 offset 1,965,620–1,973,387 共 7,768 个，**重复 0、断号 0**

### 2. 采集中断（断网或 429 限流）

- 现象：Grafana 卡片不再更新；处理延迟和新鲜度停住；告警引擎收到 P1「采集中断」
- 定位：`docker exec postgres psql -U flight -d flightdb -c "SELECT now()-data_clock FROM rt_sync_state"`，超过 30 秒说明没有新快照；再看采集器日志里的 429 或请求异常
- 恢复：重启采集器（或等额度恢复）。Kafka 里的积压会在恢复后的下一批被追上
- 验证标准：约 30 秒时收到 P1 告警，恢复后告警自动关闭，数据自动续上
- 演练记录：2026-10-21 07:33:09 停采集器，**07:33:42 写入 P1**（33 秒），07:33:47 重启采集器，**07:33:57 自动关闭**（10 秒）

### 3. 批任务失败后重跑

- 现象：Airflow 任务红色、重试后仍失败；`dq_checks` 强规则失败时链会停在这一步
- 定位：Airflow 日志里 spark-submit 的报错；质量失败看 `dq_result` 的 `detail`
- 恢复：修好后重跑。`dws_batch.py` 按区间 `replaceWhere`、`ads_export.py` 按区间 MERGE，同一区间重跑结果不变；表结构变化时会自动整表重建
- 验证标准：同一区间连续跑两次，行数和校验和一致
- 演练记录：2026-10-21 `backfill` 2026-10-20 跑两遍，行数都是 565 / 1,363 / 27 / 61，随后 `verify` 0 差异

### 4. PostgreSQL 宕机

- 现象：实时作业写服务层失败后停下（湖里数据完好）；告警引擎连不上库，每 10 秒重连一次
- 定位：`docker compose ps postgres`；实时作业日志里是 JDBC 连接异常
- 恢复：`docker start postgres`，等健康后重启实时作业。启动时先按湖全量对齐 PostgreSQL，再从 checkpoint 重放失败的那一批
- 验证标准：发布失败但湖内数据不受影响，恢复后能补发
- 演练记录：2026-10-21 07:36:12 停 postgres，实时作业随即失败退出；07:36:24 恢复，07:36:36 重启作业。之后全量对账（`rt_pipeline.py verify --start "2026-10-21 07:00" --end "2026-10-21 07:40"`）四项全部通过：起降事件与航段 567 个、最新状态 6,952 行、PostgreSQL 三张表（6,952 / 13,328 / 2,737 行）与湖逐行一致，只在一边 0、主键重复 0

### 5. 流式 MERGE 与回刷同时写一张表

- 现象：
- 定位：
- 恢复：`dwd_state.py` 的 MERGE 遇到 Delta 并发冲突会自动重试 3 次；回刷失败直接重跑，它按区间覆盖，重跑结果一样
- 验证标准：记录 Delta 并发冲突的现象，以及错开分区或暂停流之后的结果
- 演练记录：未演练。M4 之后回刷的是 DWS 表、实时作业写的是 DWD/ADS，两者不再写同一张表；真正会重叠的是 `ads_export` 的告警 MERGE 和 `rt_pipeline` 的最新状态表，它们写的是不同表。并发冲突的兜底仍是 Delta 的重试 + 按区间覆盖

### 6. 迟到数据超过水位线

- 现象：只有可选的流式聚合作业（`dws_gold_stream.py`）用 watermark；实时主链路不用窗口聚合，没有水位线，迟到数据只要还在 Kafka 里就会被处理
- 定位：`grep numRowsDroppedByWatermark` 作业日志
- 恢复：批链路的 DWS 按区间重算，迟到数据会在下一次 `backfill` 里补上
- 验证标准：查看 numRowsDroppedByWatermark，确认批链路重算后口径一致
- 演练记录：不适用于主链路；`dws_gold_stream.py` 当前不服务看板，未演练

### 7. 误开了两个实时作业

- 现象：2026-10-20 冒烟测试时，第一个作业用输出重定向启动，窗口里没有输出，又启动了第二个。第二个启动时 Spark UI 端口从 4040 换到 4041；第一批写航段时报 ConcurrentAppendException（重试后成功），随后整个查询报 `CONCURRENT_STREAM_LOG_UPDATE ... Multiple streaming jobs detected` 退出
- 定位：`docker exec spark bash -c "ps -eo pid,etime,args | grep [r]t_pipeline"` 看容器里有几个；两个作业用同一个 checkpoint，同一批会被两边各算一次。MERGE 写的表（航段、最新状态）有并发冲突检查，追加写的表（bronze、状态明细、起降事件）没有，可能写重
- 恢复：停掉全部实时作业，查 bronze 的 (partition, offset) 和状态明细的 (icao24, event_time) 有没有重复；有重复就删掉当天实时写入的数据和实时作业的 checkpoint，从 Kafka 重新处理，起降事件和航段 rebuild
- 预防：作业启动时检查容器里有没有另一个实时作业；追加带 Delta 事务标识（流式查询 id + 表、批号），同一批提交两次时 Delta 跳过第二次
- 演练记录：见验收记录 2026-10-20

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
| 2026-10-19 | M2 | 单测（起降与航段 31 个、清洗规则 35 个），在 spark 容器里跑 | 66 个全部通过，2 分 41 秒 |
| 2026-10-19 | M2 | 首次建表（`stream --available-now`，7 月四天加 10 月 17 日的状态明细 678,570 条）与流批对账（`verify`，2026-07-01 至 2026-10-20） | 一批 30.6 秒，落地 836、起飞 660，航段 6,307 个（进行中 1,162、落地 836、丢失 4,309）；对账完全一致，只在一边的行 0，主键重复 0 |
| 2026-10-19 | M2 | 落地抽查（`sql/checks/landing_spot_check.sql`） | 20 个全部像真落地。但落地后 10 分钟内又“起飞”有 47 次（MXP 25、ZRH 13），这些起飞的时速中位数 24 km/h，是滑行时地面标记跳变；据此加了滑行抖动修正，并把丢失航段分成三类原因 |
| 2026-10-19 | M2 | 修正后删表、用流作业重建，再对账 | 一批 29.1 秒，落地 812、起飞 624，航段 6,262 个（进行中 1,158、落地 812、丢失 4,292：疑似落地未识别 193、飞出范围 2,908、信号丢失 1,191）；对账完全一致（事件 1,436、航段 6,262） |
| 2026-10-19 | M2 | rebuild（表结构一致时全量重算） | 31.2 秒，结果和流作业建的完全相同。表结构变化时用 CREATE OR REPLACE 改表失败（路径表报 DELTA_CREATE_TABLE_SCHEME_MISMATCH），改为启动时检查、提示删表重建 |
| 2026-10-19 | M2 | 修正后落地抽查 | 20 个全部像真落地；落地后 10 分钟内又“起飞”剩 22 次：13 次是 BRN 小飞机的触地复飞（真起降），6 次是客机高速滑跑时标记跳变（ZRH 5、STR 1），3 次是直升机；正常起飞最慢的 5% 从 20 km/h 升到 147 km/h。漏判估计 ZRH 1%，GVA、BSL、LYS、MUC 在 29–47%，BGY、SXB、INN 判不出落地 |
| 2026-10-19 | M2 | 丢失原因与采集范围核对（抽查 SQL 第 5 段、`sql/checks/coverage_history.sql`） | 丢失航段里 805 个是采集器停机造成的（信号丢失 661、飞出范围 119、疑似落地未识别 25）；7 月四天都在北纬 45–50°、东经 5–10° 内，10 月 17 日从第一张快照（02:55:46 UTC）起就是新范围，和代码里登记的一致 |
| 2026-10-20 | M3 | I/O 诊断（`spark/bench_rt_io.py`，数据迁到 Docker 卷前后各跑一次） | D 盘经 9p 挂进容器，小文件的建、查、改名、删比容器内慢 137–1,576 倍。迁到 Docker 卷（ext4）后：bronze 列一次日志目录（1,025 个文件）7.5 秒 → 18–50 毫秒，加载表 7.6 秒 → 41 毫秒；状态明细列一次 4.0 秒 → 11 毫秒；20 分钟回看（10.8 万行、56 个文件）5.7 秒 → 0.16 秒。同样 900 行写 Delta：追加 0.76 秒 → 0.16 秒，MERGE 1.7–1.8 秒 → 0.7–0.8 秒 |
| 2026-10-20 | M3 | 数据迁移（delta/ 126 MB、checkpoint/ 6.8 MB 拷进 Docker 卷） | 状态明细 678,570 行，和迁移前一致 |
| 2026-10-20 | M3 | 单测（清洗 35、起降与航段 31、最新状态 7、服务层 9；服务层连 postgres 容器的临时库跑） | 82 个全部通过，3 分 27 秒 |
| 2026-10-20 | M3 | 第一次冒烟测试（10 秒轮询 10 分钟，04:28–04:39 UTC，65 批） | 功能通过：起降事件与航段、最新状态、PostgreSQL 与湖逐行一致，Grafana 卡片 5 秒刷新正常。耗时不达标：每批 6.1–8.9 秒（计算 4.4–6.2、PostgreSQL 0.1–0.2、Delta 1.3–3.6），处理延迟 4.8–11.0 秒；Ctrl+C 停作业时 PySpark 打出一串 Py4JError |
| 2026-10-20 | M3 | 2026-10-17 09:30 尾部数据修复（M1 停作业时状态明细没追平：09:30:13–33 的快照在 bronze 里但没进状态明细；09:30:44 的快照当时只在 Kafka，今天才入湖） | `dwd_state.py backfill` 09:30–09:31：1,770 行 → 4,417 行；全历史对账（2026-01-01 至 2026-10-21）718,096 行与批量重算完全一致；`dwd_event_leg.py rebuild` 后起降事件 1,486 条、航段 6,904 个 |
| 2026-10-20 | M3 | 批耗时优化，云端本地压测（2 核机器，合成数据每张快照 400 架，真 Delta + 本地 PostgreSQL，只看相对变化） | 默认每批 48 秒；Delta 快照重建改用 1 个任务后 25 秒；再加上统计合并（每批少 4 个 Spark 作业）、bronze 和最新状态放到后台后 23 秒。压测中发现后台线程解析时间常量用的是 JVM 默认时区（云端机器是 UTC+8，“已离开”算错），已修复；容器默认时区是 UTC，之前没有出错 |
| 2026-10-20 | M3 | 平稳停止（spark-submit 下给整个进程组发信号，模拟 docker exec -it 里按 Ctrl+C 和 pkill） | 按一次 Ctrl+C：等当前批写完再停，退出码 0，4 秒；pkill（SIGTERM）同样平稳退出；连按两次：1 秒内退出，退出码 130，没写完的批重启后重放 |
| 2026-10-20 | M3 | 第二次冒烟测试（Delta 快照 1 个任务、统计合并、bronze 和最新状态放后台之后；10 秒轮询约 9 分钟，苏黎世中午每批约 820 条，50 批） | 对账四项全部通过：全历史状态明细 757,538 行与重算一致，起降事件 1,524、航段 7,764、最新状态 5,217 与重算一致，PostgreSQL 与湖一致；Ctrl+C 平稳停止。每批 4.3–7.1 秒、中位数约 5.4 秒，仍未稳定在 5 秒内；处理延迟 4.3–9.4 秒（P95 约 8.9 秒）。计算约 3.8 秒，其中判起降、切航段 2.3–2.9 秒；写 Delta 约 1.2 秒，其中航段和最新状态的 MERGE 0.7–1.6 秒 |
| 2026-10-20 | M3 | 第三版前 25 批（并行读 Kafka、新快照不做去重反连接、服务层和湖同时写、航段和最新状态整段替换；10 秒轮询，每批约 760 条） | 每批 3.9–5.1 秒，处理延迟 4.0–5.6 秒（上一版 4.3–9.4 秒）。贴近或超过 5 秒的批有两个来源：起降事件写入前先读表去重（0.9–1.6 秒）；Delta 每 10 次提交同步写一次 checkpoint（状态明细 0.2 → 1.1 秒，航段和最新状态 0.7 → 1.4 秒）。据此改为新快照的起降事件直接追加、四张表 checkpoint 间隔改为 50 次提交 |
| 2026-10-21 | M3 | **两小时 SLA 验收**（10 秒轮询，05:02–07:03 UTC，720 个采样、722 个数据批，平均每批 636 架、峰值 1,328 架） | **新鲜度 P50 14.0 / P95 22.9 秒**（720 个样本里 3 个超过 30 秒，最长 33.1），**处理延迟 P50 7.0 / P95 9.4 秒**（722 个批里 4 批超过 10 秒，最长 11.0），服务层原子性 34,191 次探测 0 半批 0 空表。新鲜度和处理延迟两项达标 |
| 2026-10-21 | M3 | 同一次运行的每批耗时 | P50 5.57 / P95 6.25 / P99 6.70 秒，704/722 批超过 5 秒：白天流量从凌晨每批 160 架涨到 636 架，“每批稳定小于 5 秒”只在低峰成立（03:21–04:28，每批 160–375 架时 P50 4.6 / P95 5.5 秒）。原因和下一步见 README「Known Limitations」 |
| 2026-10-21 | M3 | 验收过程中被容器 OOM 杀掉一次（第 3 轮作废） | 4 GB 上限、每批 375 架时 cgroup `oom_kill=1`，驱动进程直接消失（`bash -c "spark-submit \| tee"` 的退出码取 tee 的，所以显示 0，第一眼看不出来）。上限提到 5 GB，并给进程加 `MALLOC_ARENA_MAX=2`：JVM 堆只用了 1.3/2 GB，容器 anon 却涨到 4.6 GB，是 glibc 每线程 arena 膨胀；之后 2 小时内存稳定在 2.4–3.4 GB |
| 2026-10-21 | M3 | 另外两轮作废的原因 | 第 2 轮：重启采集器时旧进程没杀干净，两个采集器同时跑，快照间隔变成约 5 秒、额度翻倍；第 4 轮：加 `MALLOC_ARENA_MAX=2` 需要重启作业，采样区间不连续，重开一轮 |
| 2026-10-21 | M4 | DWS 四张表全量重建（`dws_batch.py`，2026-07-13 19:40 至 2026-10-21 07:00） | dws_airport_5min 3,115 行、gold_country_5min 6,722、gold_overview_5min 166、dws_airport_hourly 233；5 分钟表 12.1 秒、小时表 1.9 秒。gold_country_5min / gold_overview_5min 是 M4 前的旧结构，启动时检测到列名变化自动整表重建 |
| 2026-10-21 | M4 | 区间对账（`verify --start "2026-10-20 00:00" --end "2026-10-21 00:00"`） | 四张表全部 0 差异：airport_5min 565 行、country_5min 1,363、overview_5min 27、airport_hourly 61。注意：拿“全量重建”的结果和“单日重算”比时，avg 类列可能差 0.1（浮点求和顺序不同）；同一区间比较才是精确的，下面一行就是这个练法 |
| 2026-10-21 | M4 | 批任务幂等 + 补数演练 | `backfill` 同一天（2026-10-20）跑两遍，每遍行数相同（565 / 1,363 / 27 / 61），随后 `verify` 仍 0 差异；区间外的数据一个文件没动。表结构变化时任务自动整表重建，带区间的补数遇到结构不一致会报错退出，避免冲掉历史 |
| 2026-10-21 | M4 | ADS 重建与增量 MERGE（`ads_export.py`） | 首次整表重建 5 张表（告警 1,388 条）；随后增量只合并 06:00–07:00 区间（流量 252、小时表 49、国家 519、KPI 12、告警 75 行），告警表整表仍是 1,388 条——区间外历史保留，区间内按源对齐；五张表都写进了 PostgreSQL |
| 2026-10-21 | M4 | 质量门禁（`dq_checks.py --start "2026-10-21 05:00" --end "2026-10-21 07:00"`） | 9 条规则全部通过：快照完整率 715/720（99.3%）、状态与事件主键无重复、坐标全在采集范围内、过滤率 0.15%、247 个落地航段全部有落地事件、0 个超时仍为 IN_AIR；`ads_gate` 放行。结果写进 Delta 的 dq_result 和 PostgreSQL |
| 2026-10-21 | M4 | 单测（M4 新增 DWS 口径 11 个、告警引擎 9 个，加上原有测试） | 95 通过、9 跳过（要 PostgreSQL 的两组默认跳过），4 分 59 秒；DWS 口径测试单独跑 11 个 10 秒 |
| 2026-10-21 | M5 | 质量门禁接线 | `dq_checks.py` 在 DWS 之后、ADS 之前，强规则失败退出码 1，Airflow 停在这步；`ads_export.py` 另外读最近一次 `ads_gate` 兜底（手动跑时不经过 DAG 也不会发布坏数据），带 `--ignore-dq` 才能强行发布。2026-10-21 05:00–07:00 区间实测 9 条全过、门禁放行 |
| 2026-10-21 | M5 | **告警演练：停掉采集器** | 07:33:09 停采集器，**07:33:42 写入 P1「采集中断」**（33 秒，阈值 30 秒 + 15 秒轮询），07:33:47 恢复采集，**07:33:57 自动写 `cleared_at`**（10 秒）。告警详情：“数据时钟 466 秒没有前进（阈值 30 秒），Kafka 没有新快照进来” |
| 2026-10-21 | M5 | 调试中修掉的两个误报 | ① 「流作业停止」原来只看 `rt_batch_log` 的时间，采集一停就没有空批写入，会把采集中断误报成作业停止；改成看进程是否存活（容器内 `pgrep -f jobs/rt_pipeline.py`），在容器外跑时退回看批的时间。② 「微批积压」在作业停掉后仍拿最后 3 个旧批判断，改成只看最近 2 分钟 |
| 2026-10-21 | M5 | 告警引擎单测（`tests/test_alert_engine.py`） | 10 个全部通过，0.7 秒：各规则触发、追积压的长批不算停止、应急代码按航段去重、条件恢复自动写 cleared_at、质量强规则失败、卡片超时要持续 1 分钟 |
| 2026-10-21 | M6 | **exactly-once 演练**（SIGKILL 后从 checkpoint 重启） | 07:34:42 强杀，07:35:20 重启；从 07:34:00 起 partition 0 的 7,768 个 offset **重复 0、断号 0** |
| 2026-10-21 | M6 | **PostgreSQL 宕机演练** | 07:36:12 停库，实时作业随即失败退出；07:36:24 恢复、07:36:36 重启作业；随后全量对账四项全部通过（状态明细、起降事件与航段 567 个、最新状态 6,952 行、PostgreSQL 三张表 6,952 / 13,328 / 2,737 行） |
| 2026-10-21 | M6 | 服务层与湖的全量对账（`rt_pipeline.py verify --start "2026-10-21 07:00" --end "2026-10-21 07:40"`） | 四项通过，退出码 0：只在一边的行 0、主键重复 0 |
| 2026-10-21 | M6 | CI 接入单测 | `.github/workflows/ci.yml` 新增 tests job：Java 17 + pyspark 3.5.3 + pytest，跑 `tests/`（要 PostgreSQL 的两组自动跳过） |
| 2026-10-21 | M6 | runbook 故障场景 | 1–4 有演练记录（强杀、采集中断、批任务重跑、PG 宕机），5–6 写明为什么不适用；7 沿用 M3 的记录 |
| 2026-10-21 | M6 | 应急代码告警演练 | 向 `flights` 主题注入一条 `icao24=000000`、squawk 7700 的测试报文（一架新飞机，切出航段 `000000_1792597202`），**07:40:58 写入 P1「应急代码」**；再注入同一航段同一代码的报文，`alert_event` 里仍然只有 1 条（按航段去重生效）。测试飞机 1 分钟后被标记“已离开”，不再出现在看板上 |
| 2026-10-21 | M6 | PG 宕机演练中发现并修掉的问题 | 告警引擎的重连写在 `except` 里，数据库还没起来时重连自己抛异常，会把整个引擎带退出（演练后才发现引擎已经死在 07:36）。改成重连再包一层 try，失败就 10 秒后重试 |
