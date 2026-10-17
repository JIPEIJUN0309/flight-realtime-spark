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

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("flight-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
