"""pytest 公共配置：本地 SparkSession（这些测试不需要 Delta），并让测试能 import spark/ 下的模块。"""

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 仓库里作业代码在 spark/；spark 容器里挂载在 /opt/spark/work-dir/jobs
for path in (os.path.join(ROOT, "spark"), "/opt/spark/work-dir/jobs"):
    if os.path.isdir(path) and path not in sys.path:
        sys.path.insert(0, path)

AIRPORTS_CSV = os.path.join(ROOT, "dim", "airports.csv")

# 造测试数据时会起 Python worker，让它和跑 pytest 的是同一个解释器，避免版本不一致
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("flight-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # 流式模拟测试会连续跑几百个查询，少保留历史执行计划，免得 driver 内存被占满
        .config("spark.sql.ui.retainedExecutions", "5")
        .config("spark.ui.retainedJobs", "20")
        .config("spark.ui.retainedStages", "20")
    )
    # 服务层测试要用 PostgreSQL 的 JDBC 驱动。spark 容器里它已在 /opt/spark/jars；
    # 在别处跑时用 SPARK_TEST_CLASSPATH 指向 postgresql-*.jar
    if os.getenv("SPARK_TEST_CLASSPATH"):
        builder = builder.config("spark.driver.extraClassPath", os.environ["SPARK_TEST_CLASSPATH"])
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
