# 测试

单元测试覆盖 [docs/requirements.md](../docs/requirements.md) 里的口径和验收方式。

| 文件 | 覆盖 |
| --- | --- |
| `test_flight_rules.py` | 清洗与去重：有效性边界、先过滤后去重、保留 offset 最小的一条；派生字段（飞行阶段、八方位、单位换算）；Haversine 与最近机场（含距离相同时的取舍）；dt 取 UTC 日期；按到达顺序分微批"只插入不存在的主键"与一次性批量清洗结果一致 |
| `test_flight_legs.py` | 起降判定：翻转间隔 60/61 秒、离机场 5.0/5.1 km 的边界；滑行抖动：时速 99.9/100 km/h、离机场 5.0/5.1 km 的边界，速度为空不改，一直报"空中"的飞机减速时判落地；航段切分：间隔 19/20/21 分钟、飞出范围再进入、落地后再起飞、落地或起飞没判出来、起飞事件关闭进行中的航段、按数据时间判超时；丢失原因：离机场 10.0/10.1 km、高度 999/1,000 m、是否下降，离边界约 16.7/22.2 km，7 月和 10 月用各自的采集范围，疑似落地优先于飞出范围；分批流式处理（150 秒一批）与全量重算结果一致；微批重放（含"事件写入后航段 MERGE 失败"）结果不变 |

计划中：

- [x] 清洗规则：有效性、去重、派生字段
- [x] Haversine 距离与最近机场
- [ ] 进近口径：高空下降路过、低空平飞、低空下降三类样本，只有低空下降计入
- [x] 航段切分：间隔 19 分钟和 21 分钟、飞出采集范围再进入、落地后再起飞
- [x] 起降判定：on_ground 翻转判定，相邻两条间隔超过 1 分钟时不判翻转
- [ ] 告警阈值边界：爬升 25 m/s、下降 −30 m/s 等

## 运行

测试只用本地 SparkSession，不需要 Kafka 和 Delta。清洗规则的测试约 30 秒；起降与航段的测试要模拟几十个微批，约 3 分钟。

在 spark 容器里跑（容器里已有 pyspark 和 Java，只需装一次 pytest，约 1 MB）：

```
docker cp tests spark:/opt/spark/work-dir/
docker exec -it spark bash -c "pip install -q pytest && cd /opt/spark/work-dir && python3 -m pytest -q tests"
```

改了测试之后重新 `docker cp` 一次。本机装了 Java 和 pyspark 3.5 的话，直接 `pytest tests/` 也行。
