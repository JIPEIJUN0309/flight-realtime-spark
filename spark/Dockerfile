# ============================================================
# Spark 3.5.3 + Delta Lake 3.3.2 + Kafka 连接器
# 版本严格匹配（这是坑最少的稳定组合）：
#   Spark 3.5.3  ←→  delta-spark 3.3.2  ←→  Scala 2.12
# 基于官方 Spark 镜像加一层，把所需 JAR 依赖预先下载进镜像，
# 这样运行时不用每次联网拉包，也避免版本错配。
# ============================================================
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root

# 安装 Python 依赖：delta-spark 提供 Python API（configure_spark_with_delta_pip 等）
RUN pip install --no-cache-dir delta-spark==3.3.2

# 预下载所有需要的 JAR 到 Spark 的 jars 目录
# 这样 spark-submit 时不用 --packages 联网下载
WORKDIR /opt/spark/jars

# 用 curl 从 Maven 中央仓库拉取（这些坐标版本严格对应 Spark 3.5 / Scala 2.12）
RUN set -e; \
    BASE="https://repo1.maven.org/maven2"; \
    curl -fSL -o delta-spark_2.12-3.3.2.jar          "$BASE/io/delta/delta-spark_2.12/3.3.2/delta-spark_2.12-3.3.2.jar"; \
    curl -fSL -o delta-storage-3.3.2.jar             "$BASE/io/delta/delta-storage/3.3.2/delta-storage-3.3.2.jar"; \
    curl -fSL -o spark-sql-kafka-0-10_2.12-3.5.3.jar "$BASE/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar"; \
    curl -fSL -o spark-token-provider-kafka-0-10_2.12-3.5.3.jar "$BASE/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar"; \
    curl -fSL -o kafka-clients-3.5.1.jar             "$BASE/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar"; \
    curl -fSL -o commons-pool2-2.11.1.jar            "$BASE/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"; \
    curl -fSL -o postgresql-42.7.10.jar              "$BASE/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar"

WORKDIR /opt/spark/work-dir

# 保持容器常驻，方便我们用 docker exec 进去手动 spark-submit
CMD ["tail", "-f", "/dev/null"]
