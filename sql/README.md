# SQL

数据质量检查和对账用的 SQL。建表语句（DDL）后续放进 `ddl/`。

| 文件 | 作用 |
| --- | --- |
| `checks/exactly_once_offsets.sql` | 验证 Kafka 到 ODS 恰好一次：offset 无重复、无断号 |
| `checks/zrh_ground_coverage.sql` | ZRH 5 km 内的地面记录，以及按起降规则每天判得出多少次起降，判断起降判定能否成立 |

spark 容器只挂载了 `spark/`、`dim/` 和数据目录（`.env` 的 `DATA_DIR`），sql/ 不在容器里。

不用改参数的检查，把 sql/ 拷进容器后整个文件执行（拷进去的是副本，文件改了要重新拷）：

```
docker cp sql spark:/opt/spark/work-dir/
docker exec spark /opt/spark/bin/spark-sql --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.session.timeZone=UTC --hiveconf hive.cli.print.header=true -f /opt/spark/work-dir/sql/checks/zrh_ground_coverage.sql
```

要先改参数的（例如 `exactly_once_offsets.sql` 的演练开始时间），进入 spark-sql 交互界面，改好后粘贴执行：

```
docker exec -it spark /opt/spark/bin/spark-sql --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.sql.session.timeZone=UTC
```

时间一律按 UTC 写和看（`spark.sql.session.timeZone=UTC`），北京时间减 8 小时。
