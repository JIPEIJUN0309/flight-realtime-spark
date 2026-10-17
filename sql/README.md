# SQL

数据质量检查和对账用的 SQL。建表语句（DDL）后续放进 `ddl/`。

| 文件 | 作用 |
| --- | --- |
| `checks/exactly_once_offsets.sql` | 验证 Kafka 到 ODS 恰好一次：offset 无重复、无断号 |
| `checks/zrh_ground_coverage.sql` | 统计 ZRH 5 km 内的地面记录，判断起降判定能否成立 |

spark 容器只挂载了 `spark/`、`dim/` 和 `data/`，所以先进入 spark-sql 交互界面，再粘贴执行：

```
docker exec -it spark /opt/spark/bin/spark-sql --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```
