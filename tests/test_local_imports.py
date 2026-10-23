"""导入冒烟：本地模块之间的 import 必须真的存在。

为什么单独写一个：CI 只跑 pytest + 语法检查，发现不了
`from ads_export import merge_into_delta` 这种「名字被删了、导入还在」的错误——
语法没问题，只有真正 import 时才炸（项目报告 H2）。

这里用 AST 静态检查：spark/*.py 里 `from X import name` 只要 X 是同目录的模块，
name 就必须在 X 里定义过（函数、类、赋值、带注解的赋值）。
真实运行时的 import 还要装 pyspark/delta/torch，太重；静态检查足够拦住这一类。
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SPARK_DIR = Path(__file__).resolve().parent.parent / "spark"


def defined_names(path: Path) -> set[str]:
    """模块顶层定义的名字：def / class / 赋值 / 带注解赋值。"""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.asname or alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                names.add(alias.asname or alias.name)
    return names


def test_local_imports_resolve() -> None:
    modules = {p.stem: p for p in SPARK_DIR.glob("*.py")}
    cache = {name: defined_names(path) for name, path in modules.items()}

    missing: list[str] = []
    for path in modules.values():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            # 只查同目录模块（ml_common、ads_export 这种），第三方包交给运行时
            if node.level or node.module not in modules:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                if alias.name not in cache[node.module]:
                    missing.append(
                        f"{path.name}: from {node.module} import {alias.name}"
                    )

    assert not missing, "这些本地导入的名字不存在：\n" + "\n".join(missing)

def test_dag_puts_the_quality_gate_before_ads() -> None:
    """质量门禁必须卡在 DWS 和 ADS 之间（项目报告 H1）。

    直接读 DAG 源码断言依赖链：pytest 环境里没有 Airflow，装它太重；
    但这条链一旦被改回去，门禁就又形同虚设，值得用一行文本断言守住。
    """
    # 仓库里在 <root>/airflow/dags；spark 容器只挂了 spark/ 和 dim/，
    # 找不到 DAG 就跳过（CI 里是完整仓库，会真的检查）
    dag_file = Path(__file__).resolve().parent.parent / "airflow/dags/flight_batch_pipeline.py"
    if not dag_file.exists():
        pytest.skip(f"当前布局没有 DAG 文件：{dag_file}")
    src = dag_file.read_text(encoding="utf-8")
    assert "preflight >> dws_batch >> dq_checks >> ads_export" in src
    assert "preflight >> dws_batch >> ads_export" not in src
