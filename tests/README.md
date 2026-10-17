# 测试

计划中的单元测试，来自 [docs/requirements.md](../docs/requirements.md) 的验收方式：

- [ ] 进近口径：高空下降路过、低空平飞、低空下降三类样本，只有低空下降计入
- [ ] 航段切分：间隔 19 分钟和 21 分钟、飞出采集范围再进入、落地后再起飞
- [ ] 起降判定：on_ground 翻转判定，相邻两条间隔超过 1 分钟时不判翻转
- [ ] Haversine 距离与最近机场
- [ ] 告警阈值边界：爬升 25 m/s、下降 −30 m/s 等

运行：`pytest tests/`（需要 pyspark 和 delta-spark，用本地 SparkSession）。
