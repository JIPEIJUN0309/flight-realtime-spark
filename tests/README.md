# 测试

单元测试覆盖 [docs/requirements.md](../docs/requirements.md) 里的口径和验收方式。

| 文件 | 覆盖 |
| --- | --- |
| `test_flight_rules.py` | 清洗与去重：有效性边界、先过滤后去重、保留 offset 最小的一条；派生字段（飞行阶段、八方位、单位换算）；Haversine 与最近机场（含距离相同时的取舍）；dt 取 UTC 日期；按到达顺序分微批"只插入不存在的主键"与一次性批量清洗结果一致 |

计划中：

- [x] 清洗规则：有效性、去重、派生字段
- [x] Haversine 距离与最近机场
- [ ] 进近口径：高空下降路过、低空平飞、低空下降三类样本，只有低空下降计入
- [ ] 航段切分：间隔 19 分钟和 21 分钟、飞出采集范围再进入、落地后再起飞
- [ ] 起降判定：on_ground 翻转判定，相邻两条间隔超过 1 分钟时不判翻转
- [ ] 告警阈值边界：爬升 25 m/s、下降 −30 m/s 等

## 运行

测试只用本地 SparkSession，不需要 Kafka 和 Delta，一次约 30 秒。

在 spark 容器里跑（容器里已有 pyspark 和 Java，只需装一次 pytest，约 1 MB）：

```
docker cp tests spark:/opt/spark/work-dir/
docker exec -it spark bash -c "pip install -q pytest && cd /opt/spark/work-dir && python3 -m pytest -q tests"
```

改了测试之后重新 `docker cp` 一次。本机装了 Java 和 pyspark 3.5 的话，直接 `pytest tests/` 也行。
