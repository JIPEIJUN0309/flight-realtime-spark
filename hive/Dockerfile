# ============================================================
# 阶段七：Hive on Tez（用来和 Spark 做同逻辑性能对比）
# ------------------------------------------------------------
# 在官方 apache/hive 镜像上只加一件事：塞入 PostgreSQL JDBC 驱动。
# 为什么需要它？
#   我们用真实的 Postgres 当 Hive 元数据库（metastore RDBMS），
#   而不是玩具级的内嵌 Derby——这是"接近真实"的关键一步。
#   Hive 连 Postgres 元数据库要用 JDBC 驱动，官方镜像默认只带 Derby，
#   所以这里把 postgres 驱动预先放进 /opt/hive/lib。
#
# 说明：apache/hive:4.0.1 默认执行引擎就是 Tez（不是老的 MapReduce），
# 所以这套天生就是"Hive on Tez"，正好对上你淘宝项目的技术栈。
# ============================================================
FROM apache/hive:4.0.1

USER root

# 用 ADD 直接从 Maven 中央仓库拉 JDBC 驱动（ADD 支持 URL，省得装 curl）。
# 版本和主栈 spark/Dockerfile 里那颗 postgres 驱动保持一致（42.7.10）。
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.10/postgresql-42.7.10.jar \
    /opt/hive/lib/postgresql-42.7.10.jar
RUN chmod 644 /opt/hive/lib/postgresql-42.7.10.jar

# 装回镜像默认的 hive 用户
USER hive
