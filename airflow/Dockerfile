# ============================================================
# 阶段（可选）：Airflow 编排下游批处理
# ------------------------------------------------------------
# 在官方 Airflow 镜像上加一层，只多做一件事：装入 docker CLI。
# 为什么需要 docker CLI？
#   本项目所有 Spark 作业都是用
#       docker exec spark /opt/spark/bin/spark-submit <job>.py
#   跑的（见《接力速查手册》3.3）。Airflow 要自动化这套流程，
#   最忠实的方式就是让它在 DAG 里敲同样的命令——所以 Airflow
#   容器里需要一个 docker 客户端，通过挂载的 /var/run/docker.sock
#   去操控宿主机上已经在跑的 spark 容器。
#
# 注意：这里只装 docker“客户端”（一个静态二进制），不装 Docker
# 引擎。它本身不跑容器，只是把命令发给宿主的 Docker 守护进程。
# ============================================================
FROM apache/airflow:2.10.5

# 切到 root 才能往 /usr/local/bin 写、装系统包
USER root

# 从 Docker 官方静态构建里只取出 docker 客户端二进制，扔进 PATH。
# 比 apt 装 docker.io 轻得多（不拉整个引擎），也不用加 apt 源。
ARG DOCKER_CLI_VERSION=27.5.1
RUN set -eux; \
    curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" \
      | tar -xz -C /usr/local/bin --strip-components=1 docker/docker; \
    docker --version

# 装回非 root 的 airflow 用户（uid 50000，主组 root/0）。
# 主组是 0 很关键：Docker socket 通常是 root:root、660 权限，
# 属于 0 组的进程才能读写它——这样默认用户就能 docker exec，
# 不必把整个容器跑成 root。
USER airflow
