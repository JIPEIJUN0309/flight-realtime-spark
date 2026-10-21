# 测试

单元测试覆盖 [docs/requirements.md](../docs/requirements.md) 里的口径和验收方式。

| 文件 | 覆盖 |
| --- | --- |
| `test_flight_rules.py` | 清洗与去重：有效性边界、先过滤后去重、保留 offset 最小的一条；派生字段（飞行阶段、八方位、单位换算）；Haversine 与最近机场（含距离相同时的取舍）；dt 取 UTC 日期；按到达顺序分微批"只插入不存在的主键"与一次性批量清洗结果一致 |
| `test_flight_legs.py` | 起降判定：翻转间隔 60/61 秒、离机场 5.0/5.1 km 的边界；滑行抖动：时速 99.9/100 km/h、离机场 5.0/5.1 km 的边界，速度为空不改，一直报"空中"的飞机减速时判落地；航段切分：间隔 19/20/21 分钟、飞出范围再进入、落地后再起飞、落地或起飞没判出来、起飞事件关闭进行中的航段、按数据时间判超时；丢失原因：离机场 10.0/10.1 km、高度 999/1,000 m、是否下降，离边界约 16.7/22.2 km，7 月和 10 月用各自的采集范围，疑似落地优先于飞出范围；分批流式处理（150 秒一批）与全量重算结果一致；微批重放（含"事件写入后航段 MERGE 失败"）结果不变 |
| `test_latest_position.py` | 最新状态：每架飞机取最新一条、晚到的旧记录不覆盖、按数据时钟标记已离开（60/61 秒边界）、重新出现时清除已离开、不受机器时间影响；10 秒和 40 秒一批流式处理与全量重算一致；同一批重放不产生改动 |
| `test_dws_metrics.py` | DWS 口径：进近三类样本（高空下降路过 / 低空平飞 / 低空下降）只有低空下降计入，30 km、3,000 m、±1 m/s 的闭区间边界；架数是去重飞机数而不是记录数；5 分钟窗口边界切分；小时起降按事件数（同一架飞机一小时 3 架次只算 3）、不同机场分开；国家占比合计 100%；全区域总览的阶段计数；区间对齐到 5 分钟/整点（重算幂等的前提） |
| `test_alert_engine.py` | 告警引擎（需要一个测试库，设置 `ALERT_TEST_DSN` 才跑）：健康时 0 条告警；流作业停止 15 秒、采集中断 30 秒、管道停了不重复报采集中断、连续 3 个慢批才算积压；应急代码按航段去重、已落地不再报、没有航段时退回按飞机去重；条件恢复后自动写 `cleared_at`；质量强规则失败；卡片超时要持续 1 分钟才报 |
| `test_pg_sync.py` | 服务层（需要 PostgreSQL，设置 `PG_TEST_URL` 才跑）：建表与只读账号、旧表结构自动替换、CSV 往返不丢值（逗号、引号、重音字母、空串与空值、微秒）、微批 upsert 与同步序号、事件只插入而全量对齐以湖为准、数据时钟只往后走、进近口径三类样本只有低空下降计入；原子性：并发读者 12 批里从没看到半批，非事务写入会被探针抓到 |

计划中：

- [x] 清洗规则：有效性、去重、派生字段
- [x] Haversine 距离与最近机场
- [x] 进近口径：高空下降路过、低空平飞、低空下降三类样本，只有低空下降计入（实时卡片的视图见 `test_pg_sync.py`，DWS 的批口径见 `test_dws_metrics.py`）
- [x] 航段切分：间隔 19 分钟和 21 分钟、飞出采集范围再进入、落地后再起飞
- [x] 起降判定：on_ground 翻转判定，相邻两条间隔超过 1 分钟时不判翻转
- [x] DWS 口径：5 分钟窗口、小时起降、架数/架次、区间对齐
- [ ] 告警阈值边界：爬升 25 m/s、下降 −30 m/s 等

## 运行

测试只用本地 SparkSession，不需要 Kafka 和 Delta。清洗规则的测试约 30 秒；起降与航段的测试要模拟几十个微批，约 3 分钟。

在 spark 容器里跑（容器里已有 pyspark 和 Java，只需装一次 pytest，约 1 MB）：

```
docker cp tests spark:/opt/spark/work-dir/
docker exec -it spark bash -c "pip install -q pytest && cd /opt/spark/work-dir && python3 -m pytest -q tests"
```

服务层的测试要连 PostgreSQL：加上 `-e PG_TEST_URL=jdbc:postgresql://postgres:5432/flightdb_test`，测试会在 postgres 里临时建这个库、测完删掉（库名必须以 `_test` 结尾）。不设置就跳过。

告警引擎的测试同样要连 PostgreSQL，用 psycopg2（spark 镜像里已装）：

```
docker exec -e ALERT_TEST_DSN="host=postgres port=5432 dbname=flightdb_test user=flight password=flight123" \
    spark bash -c "cd /opt/spark/work-dir && python3 -m pytest -q tests/test_alert_engine.py"
```

改了测试之后重新 `docker cp` 一次。本机装了 Java 和 pyspark 3.5 的话，直接 `pytest tests/` 也行。
