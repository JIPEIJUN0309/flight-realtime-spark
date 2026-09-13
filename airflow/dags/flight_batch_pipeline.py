"""
阶段（可选）：Airflow 自动化下游批处理
=========================================================
把《接力速查手册》3.4 里"手动跑一遍才能刷新大屏"的流程，
自动化成一条定时调度的 DAG。

手动流程（原来要人肉依次敲）：
    dwd_silver.py  →  dws_gold_batch.py  →  ads_export.py
                   →  ml_features.py     →  ml_score.py

这条 DAG 做的就是同一件事，只是让 Airflow 按时、按依赖、带重试地跑，
并且每一步的日志、成败、耗时都留痕，可在 Web UI 里回看。

★ 设计上的关键点
  1. 只编排【批处理】层。ODS(ods_bronze.py) 是常驻流作业，不归 Airflow 管，
     它和采集器 opensky_producer.py 一样是"一直开着的水龙头"。
  2. 每个任务本质就是敲一条 `docker exec spark spark-submit ...`——
     和你手动跑的命令一字不差，Airflow 只是替你敲。
  3. 严格串行 + max_active_runs=1：因为 dwd/dws 是 overwrite 写，
     两次 run 并行会互相踩踏，必须一次只跑一条链。

★ 异常检测模块（阶段九）在这条链里的位置
  只有【推理】进调度，【训练】不进。原因是两者的节奏完全不同：
    - ml_features + ml_score 是每批新数据都要重跑的，属于流水线；
    - ml_train + ml_eval 是模型迭代动作，几天到几周才跑一次，
      而且要人看着 sweep 结果和 PR-AUC 决定要不要换模型。
  把训练塞进 15 分钟一次的调度，等于每 15 分钟悄悄换一次模型，
  线上打分口径天天变，出了问题根本回溯不了。
  这就是 MLOps 里训练管道和推理管道分开的基本理由。
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
MODEL_CONFIG = "/data/ml/model/config.json"


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
    description="DWD→DWS→ADS→异常检测 下游批处理，自动刷新 Metabase 大屏",
    default_args=default_args,
    # 每 15 分钟跑一遍。想改频率就动这里；想只手动触发就设 schedule=None。
    # 前提：采集器 + ODS 流作业得开着，否则没有新数据可加工。
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,             # 不补跑历史，只跑当下
    max_active_runs=1,         # ★ 一次只允许一条链在跑，避免 overwrite 互相踩踏
    tags=["flight", "batch", "delta", "spark", "ml"],
    doc_md=__doc__,
) as dag:

    # ---- 0) 预检：确认常驻的 spark 容器可达 ----
    #   spark 没起（比如主栈没 up），后面几步必然失败。
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

    # ---- 4) 异常检测 · 特征抽取 ----
    ml_features = BashOperator(
        task_id="ml_features",
        bash_command=submit("ml_features.py"),
        doc_md="从 silver 抽每架飞机的连续航迹序列，去重 + 连续性筛选后写 Parquet。",
    )

    # ---- 5) 异常检测 · 模型打分 ----
    #   模型还没训练过时不报错，跳过即可 —— 这条链的其余部分不该被
    #   "还没训模型"这件事拖垮。训练是手动动作，见本文件顶部说明。
    ml_score = BashOperator(
        task_id="ml_score",
        bash_command=(
            f"if docker exec spark test -f {MODEL_CONFIG}; then "
            f"  {submit('ml_score.py')}; "
            "else "
            "  echo '⏭  未找到已训练模型，跳过模型打分。'; "
            "  echo '   先手动跑一次： docker exec -it spark python3 "
            f"{JOBS_DIR}/ml_train.py  然后  ml_eval.py'; "
            "fi"
        ),
        doc_md=(
            "LSTM-AE 推理打分，结果 MERGE 进 ads_ml_anomaly 并写 PostgreSQL。"
            "模型不存在时跳过而非失败。"
        ),
    )

    # 依赖：严格串行。任一步失败，后面的不会跑。
    preflight >> dwd_silver >> dws_gold >> ads_export >> ml_features >> ml_score
