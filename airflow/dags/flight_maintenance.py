"""Delta 表维护 DAG：每天 VACUUM 一次、给追加型表 OPTIMIZE。

为什么单独一条 DAG（项目报告 M8）
---------------------------------
批链路每 15 分钟重写航段和最新状态表，Delta 只打删除标记、不删物理文件；bronze 和
状态明细又是每批追加一个小文件。不维护的话，表体积和文件数会一直涨，读表越来越慢
（架构文档里航段表 121 MB，比 80 万行的状态明细还大）。

这条 DAG 和批链路分开：
- 频率不同（每天一次 vs 每 15 分钟）；
- VACUUM 会删物理文件，不适合和正在追加的批任务挤在同一条链里；
- 失败不影响看板，第二天会再跑。

手动跑：
    docker exec -it spark /opt/spark/bin/spark-submit \
        /opt/spark/work-dir/jobs/delta_maintenance.py --retain-hours 168
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_SUBMIT = "docker exec spark /opt/spark/bin/spark-submit"
JOBS_DIR = "/opt/spark/work-dir/jobs"

default_args = {
    "owner": "flight",
    "retries": 1,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(minutes=60),
}

with DAG(
    dag_id="flight_maintenance",
    description="Delta 表维护：VACUUM（保留 7 天）+ 追加型表 OPTIMIZE",
    default_args=default_args,
    # 每天 UTC 02:30，欧洲空域流量最低的时段
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2026, 10, 22, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["flight", "delta", "maintenance"],
    doc_md=__doc__,
) as dag:

    preflight = BashOperator(
        task_id="preflight_check_spark",
        bash_command=(
            "docker exec spark true 2>/dev/null "
            "|| { echo '❌ spark 容器不可达：请先启动主栈 `docker compose up -d`'; exit 1; }"
        ),
    )

    vacuum = BashOperator(
        task_id="vacuum",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/delta_maintenance.py "
                     f"--retain-hours 168 --vacuum-only",
        doc_md="删掉 7 天前的旧文件（重写型表留下的删除标记）。",
    )

    optimize = BashOperator(
        task_id="optimize",
        bash_command=f"{SPARK_SUBMIT} {JOBS_DIR}/delta_maintenance.py --optimize-only",
        doc_md="把 bronze、状态明细、起降事件的小文件合并成大文件。",
    )

    preflight >> vacuum >> optimize
