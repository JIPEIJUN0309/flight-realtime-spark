"""
阶段（可选）：Airflow 自动化下游批处理
=========================================================
把《接力速查手册》3.4 里"手动跑一遍才能刷新大屏"的流程，
自动化成一条定时调度的 DAG。

手动流程（原来要人肉依次敲）：
    dwd_silver.py  →  dws_gold_batch.py  →  ads_export.py

这条 DAG 做的就是同一件事，只是让 Airflow 按时、按依赖、带重试地跑，
并且每一步的日志、成败、耗时都留痕，可在 Web UI 里回看。

★ 设计上的关键点
  1. 只编排【批处理】三层。ODS(ods_bronze.py) 是常驻流作业，不归 Airflow 管，
     它和采集器 opensky_producer.py 一样是"一直开着的水龙头"。
  2. 每个任务本质就是敲一条 `docker exec spark spark-submit ...`——
     和你手动跑的命令一字不差，Airflow 只是替你敲。
  3. 严格串行 + max_active_runs=1：因为 dwd/dws 是 overwrite 写，
     两次 run 并行会互相踩踏，必须一次只跑一条链。

★ 为什么这一步有价值（讲解思路见 STAGE_AIRFLOW_GUIDE.md）
  这正好把三个项目串成一条线：天气项目用 Airflow 调度离线批，
  本项目也用 Airflow 把湖仓的批处理层自动化——证明"生产环境里
  批处理层用 Airflow 定时调度"不是嘴上说说，而是真的落地了。
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------
# 所有 Spark 作业的统一跑法：docker exec 进常驻的 spark 容器再 spark-submit。
# 和《接力速查手册》3.3 完全一致，只是去掉了交互用的 -it。
# ---------------------------------------------------------------
SPARK_SUBMIT = "docker exec spark /opt/spark/bin/spark-submit"
JOBS_DIR = "/opt/spark/work-dir/jobs"


def submit(job: str) -> str:
    """拼出一条 spark-submit 命令。job 例如 'dwd_silver.py'。"""
    return f"{SPARK_SUBMIT} {JOBS_DIR}/{job}"


# 任务默认参数：失败重试 1 次、间隔 2 分钟；单个任务最长跑 30 分钟。
default_args = {
    "owner": "flight",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
    "execution_timeout": pendulum.duration(minutes=30),
}

with DAG(
    dag_id="flight_batch_pipeline",
    description="DWD→DWS→ADS 下游批处理，自动刷新 Metabase 大屏",
    default_args=default_args,
    # 每 15 分钟跑一遍。想改频率就动这里；想只手动触发就设 schedule=None。
    # 前提：采集器 + ODS 流作业得开着，否则没有新数据可加工。
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,             # 不补跑历史，只跑当下
    max_active_runs=1,         # ★ 一次只允许一条链在跑，避免 overwrite 互相踩踏
    tags=["flight", "batch", "delta", "spark"],
    doc_md=__doc__,
) as dag:

    # ---- 0) 预检：确认常驻的 spark 容器可达 ----
    #   spark 没起（比如主栈没 up），后面三步必然失败。
    #   先在这里快速失败并给出清晰提示，比让 spark-submit 报一堆栈好读。
    preflight = BashOperator(
        task_id="preflight_check_spark",
        bash_command=(
            "docker exec spark true 2>/dev/null "
            "|| { echo '❌ spark 容器不可达：请先启动主栈 `docker compose up -d`'; exit 1; } "
            "&& echo '✅ spark 容器在运行，开始批处理链路'"
        ),
    )

    # ---- 1) DWD：清洗 + 机场关联 → silver ----
    dwd_silver = BashOperator(
        task_id="dwd_silver",
        bash_command=submit("dwd_silver.py"),
        doc_md="读 bronze，用 Spark SQL 清洗、换算、Haversine 就近关联机场，overwrite 写 silver。",
    )

    # ---- 2) DWS：5 分钟窗口聚合 → 三张 gold 表 ----
    dws_gold = BashOperator(
        task_id="dws_gold_batch",
        bash_command=submit("dws_gold_batch.py"),
        doc_md="读 silver，按 5 分钟窗口聚合出 airport / country / overview 三张 gold 表。",
    )

    # ---- 3) ADS：MERGE 增量更新 + JDBC 写 PostgreSQL ----
    ads_export = BashOperator(
        task_id="ads_export",
        bash_command=submit("ads_export.py"),
        doc_md="读 silver+gold，算 5 张 ADS 表，Delta MERGE 后写进 PostgreSQL，喂给大屏。",
    )

    # 依赖：严格串行。任一步失败，后面的不会跑。
    preflight >> dwd_silver >> dws_gold >> ads_export
