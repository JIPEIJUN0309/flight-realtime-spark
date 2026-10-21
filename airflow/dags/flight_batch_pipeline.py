"""
下游批处理 DAG：DWS → ADS → 异常检测
=====================================
把"手动依次跑一遍 spark-submit 才能刷新大屏"的流程，自动化成一条定时调度的 DAG。

手动流程（原来要人肉依次敲）：
    dws_batch.py  →  ads_export.py  →  ml_features.py  →  ml_score.py

★ M4 之后的两个变化

  1. 不再跑 dwd_silver.py。
     状态明细 dwd_flight_state 由实时作业 rt_pipeline.py 每 5 秒写入，批链路直接读它。
     过渡期的 silver_flights 从此没有下游，可以下线（见 docs/runbook.md）。

  2. 每个任务带上本次的数据区间（--start/--end），不再"自己猜从哪算起"。
     Airflow 的 data_interval_start / data_interval_end 就是这次该处理的时段，
     补数时 `airflow dags backfill` 会给每一天/每 15 分钟生成各自的区间，
     每个区间重算自己的窗口，互不影响 —— 这就是验收里的"补数：结果与首次一致，其他日期不变"。
     往前多看 1 小时，是为了把上一条调度边界附近迟到写入的快照一起重算掉。

★ 为什么训练不进调度
  ml_features + ml_score 是每批新数据都要重跑的，属于流水线；
  ml_train + ml_eval 是模型迭代动作，几天到几周才跑一次，而且要人看着 sweep 结果
  和 PR-AUC 决定要不要换模型。把训练塞进 15 分钟一次的调度，等于每 15 分钟悄悄换一次
  模型，线上打分口径天天变，出了问题根本回溯不了。这就是 MLOps 里训练管道和推理管道
  分开的基本理由。
"""

from __future__ import annotations

import json
import os
import urllib.request

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------
# 所有 Spark 作业的统一跑法：docker exec 进常驻的 spark 容器再 spark-submit。
# ---------------------------------------------------------------
SPARK_SUBMIT = "docker exec spark /opt/spark/bin/spark-submit"
JOBS_DIR = "/opt/spark/work-dir/jobs"
MODEL_CONFIG = "/data/ml/model/config.json"

# 每个批任务的区间：从上一个调度边界再往前 1 小时，到本次调度边界。
START = ("{{ (data_interval_start - macros.timedelta(hours=1))"
         ".strftime('%Y-%m-%d %H:%M:%S') }}")
END = "{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}"


def notify_failure(context) -> None:
    """任务失败时推一条 P2（requirements.md 告警规则里的“批任务失败”）。

    没配 ALERT_WEBHOOK_URL 就只写 Airflow 日志，不影响重试。
    """
    ti = context.get("task_instance")
    text = (f"[P2] 批任务失败：{ti.dag_id}.{ti.task_id}\n"
            f"运行 {context.get('run_id')}，重试 {ti.try_number} 次，"
            f"错误：{context.get('exception')}")
    print(text)
    url = os.getenv("ALERT_WEBHOOK_URL", "")
    if not url:
        return
    kind = os.getenv("ALERT_WEBHOOK_KIND", "feishu")
    if kind == "feishu":
        payload = {"msg_type": "text", "content": {"text": text}}
    elif kind == "dingtalk":
        payload = {"msgtype": "text", "text": {"content": text}}
    else:
        payload = {"title": "flight-realtime-spark 批任务失败", "text": text}
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:  # noqa: BLE001 —— 推送失败不能盖住真正的任务异常
        print(f"[alert] 推送失败：{e!r}")


def submit(job: str, args: str = "") -> str:
    """拼一条 spark-submit 命令。job 例如 'dws_batch.py'。"""
    return f"{SPARK_SUBMIT} {JOBS_DIR}/{job}{(' ' + args) if args else ''}"


# 任务默认参数：失败重试 1 次、间隔 2 分钟；单个任务最长跑 30 分钟。
default_args = {
    "owner": "flight",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=2),
    "execution_timeout": pendulum.duration(minutes=30),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="flight_batch_pipeline",
    description="DWS→ADS→异常检测 下游批处理，自动刷新实时卡片和统计报表",
    default_args=default_args,
    # 每 15 分钟跑一遍。想改频率就动这里；想只手动触发就设 schedule=None。
    # 前提：采集器 + 实时作业开着，否则没有新数据可加工。
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 10, 18, tz="UTC"),
    catchup=False,             # 不自动补历史；补历史用 airflow dags backfill
    max_active_runs=1,         # ★ 一次只允许一条链在跑，避免区间写重
    tags=["flight", "batch", "delta", "spark", "ml"],
    doc_md=__doc__,
) as dag:

    # ---- 0) 预检：确认常驻的 spark 容器可达 ----
    preflight = BashOperator(
        task_id="preflight_check_spark",
        bash_command=(
            "docker exec spark true 2>/dev/null "
            "|| { echo '❌ spark 容器不可达：请先启动主栈 `docker compose up -d`'; exit 1; } "
            "&& echo '✅ spark 容器在运行，开始批处理链路'"
        ),
    )

    # ---- 1) DWS：状态明细 + 起降事件 → 4 张汇总表 ----
    dws_batch = BashOperator(
        task_id="dws_batch",
        bash_command=submit("dws_batch.py", f"run --start '{START}' --end '{END}'"),
        doc_md=(
            "读 dwd_flight_state（空中记录）和 dwd_airport_event，按本次区间重算 "
            "dws_airport_5min、dws_airport_hourly、gold_country_5min、gold_overview_5min。"
        ),
    )

    # ---- 2) 质量门禁：强规则失败就停在这里，ADS 不发布 ----
    dq_checks = BashOperator(
        task_id="dq_checks",
        bash_command=submit("dq_checks.py",
                            f"--start '{START}' --end '{END}' --fail-on-strong"),
        doc_md=(
            "按本次区间跑完整性、唯一性、有效性、一致性规则，结果写 dq_result。"
            "强规则失败时任务失败，后面的 ADS 不会发布（看板保留上一版）。"
        ),
    )

    # ---- 3) ADS：MERGE 进 Delta，再整表写 PostgreSQL ----
    ads_export = BashOperator(
        task_id="ads_export",
        bash_command=submit("ads_export.py", f"--start '{START}' --end '{END}'"),
        doc_md="按同一区间算 5 张 ADS 表，Delta MERGE 后写进 PostgreSQL，喂给看板。",
    )

    # ---- 4) 异常检测 · 特征抽取 ----
    ml_features = BashOperator(
        task_id="ml_features",
        bash_command=submit("ml_features.py"),
        doc_md="从 dwd_flight_state 抽每架飞机的连续航迹序列，去重 + 连续性筛选后写 Parquet。",
    )

    # ---- 4) 异常检测 · 模型打分 ----
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
    preflight >> dws_batch >> ads_export >> ml_features >> ml_score
