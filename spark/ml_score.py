"""
阶段九(5/5)：模型推理落链路 —— Delta MERGE + JDBC → PostgreSQL
========================================================================
读航迹序列 → LSTM-AE 打分 → MERGE 进 ADS 表 → 写 PostgreSQL 供 Metabase

跑法：
    docker exec -it spark /opt/spark/bin/spark-submit \
        /opt/spark/work-dir/jobs/ml_score.py

★ 这一步的意义：让模型落在链路上，而不是留在 notebook 里

  一个只能在 notebook 里跑出漂亮指标的模型，在生产里等于不存在。
  这个作业刻意复用 ads_export.py 的两个基建：
    - merge_into_delta(sync=True)：告警是从全量重算的，源就是全量真相。
      模型一换、阈值一调，旧模型算出的告警必须清掉，否则表里会像当初
      那 623 条一样，赖着一批按旧标准产生的脏数据。
    - write_to_pg：结果落 PostgreSQL，Metabase 直接查。

  产出表 ads_ml_anomaly 和现有的 ads_flight_alert 是同一时间轴上的两套判断，
  在 Metabase 里并排放，就是"规则 vs 模型"的对比面板。
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (BooleanType, DoubleType, StringType,
                               StructField, StructType, TimestampType)

import ml_common as mc
# ★ 直接复用 ADS 层已经写好的 MERGE 封装和 JDBC 写入，不再抄一份。
#   两处逻辑分叉的代价，ads_export.py 里那段 623 条陈旧告警的注释已经写过了。
from ads_export import merge_into_delta, write_to_pg, TH_RAPID_CLIMB

POINTS_PATH = "/data/ml/points"
MODEL_DIR = "/data/ml/model"
ADS_ML_PATH = "/data/delta/ads/ml_anomaly"

ALERT_TABLE = "ads_ml_anomaly"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ML-Anomaly-Score")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


RESULT_SCHEMA = StructType([
    StructField("icao24", StringType(), False),
    StructField("window_start", TimestampType(), False),
    StructField("window_end", TimestampType(), True),
    StructField("anomaly_score", DoubleType(), True),
    StructField("is_alert", BooleanType(), True),
    StructField("rule_alert", BooleanType(), True),
    StructField("top_feature", StringType(), True),
    StructField("top_feature_pct", DoubleType(), True),
    StructField("detail", StringType(), True),
])


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 66)
    print("  异常检测 · 模型推理 → ADS 层")
    print("=" * 66)

    with open(f"{MODEL_DIR}/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    if "threshold" not in cfg:
        raise SystemExit("config.json 里没有阈值，请先跑 ml_eval.py。")

    nz = np.load(f"{MODEL_DIR}/norm.npz")
    mu, sd = nz["mu"], nz["sd"]
    threshold = cfg["threshold"]
    n_debounce = cfg.get("debounce", 1)
    print(f"[score] 阈值 {threshold:.5f}（分位数 {cfg['threshold_quantile']}），"
          f"连续 {n_debounce} 窗口触发")

    import torch
    model = mc.build_model(n_features=cfg["n_features"], hidden=cfg["hidden"],
                           latent=cfg["latent"], seq_len=cfg["seq_len"])
    model.load_state_dict(torch.load(f"{MODEL_DIR}/lstm_ae.pt",
                                     map_location="cpu"))

    # ---- 用 Spark 读，转 pandas 做窗口化（数据量在十万行级，完全放得下）----
    points = spark.read.parquet(POINTS_PATH).toPandas()
    points["event_time"] = pd.to_datetime(points["event_time"])
    print(f"[score] 航迹点 {len(points)} 条 / "
          f"飞机 {points['icao24'].nunique()} 架")

    X, meta = mc.build_windows(points)
    if len(X) == 0:
        print("[score] 切不出窗口，退出。")
        spark.stop()
        return
    Xn = mc.apply_normalizer(X, mu, sd)
    score, per_feat = mc.reconstruction_error(model, Xn)
    print(f"[score] 打分窗口 {len(X)} 个")

    # ---- 阈值 + 去抖动 ----
    flag = (score >= threshold).astype(int)
    if n_debounce > 1:
        from ml_eval import debounce
        flag = debounce(meta, flag, n_debounce)

    # ---- 归因：误差占比最大的那个特征 ----
    pct = per_feat / per_feat.sum(axis=1, keepdims=True)
    top_i = pct.argmax(axis=1)
    top_feature = [mc.FEATURES[i] for i in top_i]
    top_pct = pct[np.arange(len(pct)), top_i]

    # ---- 同一批窗口上，现有规则是否也会报 ----
    rule_alert = (meta["max_vr"].to_numpy() > TH_RAPID_CLIMB)

    # ---- 组装结果 ----
    rows = []
    for i in range(len(meta)):
        if not flag[i] and not rule_alert[i]:
            continue          # 两边都不报的窗口不入表，避免表膨胀
        rows.append((
            str(meta["icao24"].iloc[i]),
            pd.Timestamp(meta["window_start"].iloc[i]).to_pydatetime(),
            pd.Timestamp(meta["window_end"].iloc[i]).to_pydatetime(),
            float(score[i]),
            bool(flag[i]),
            bool(rule_alert[i]),
            top_feature[i],
            float(top_pct[i]),
            f"重构误差 {score[i]:.4f}，主因 {top_feature[i]}"
            f"（占 {top_pct[i] * 100:.0f}%）",
        ))

    print(f"[score] 入表窗口 {len(rows)} 个")
    if not rows:
        print("        本批数据里模型和规则都没有报警。")
        spark.stop()
        return

    result = spark.createDataFrame(rows, schema=RESULT_SCHEMA)

    # ★ sync=True：和 ads_flight_alert 同理，源是全量重算的真相，
    #   换模型/调阈值后旧结果必须清掉。
    merge_into_delta(spark, result, ADS_ML_PATH,
                     ["icao24", "window_start"], ALERT_TABLE, sync=True)

    tbl = spark.read.format("delta").load(ADS_ML_PATH)
    tbl_cnt = tbl.count()
    if tbl_cnt != len(rows):
        print(f"      ⚠️  不一致！本次算出 {len(rows)} 条，表里 {tbl_cnt} 条")
    else:
        print(f"      ✓ 一致性校验通过：{tbl_cnt} 条")

    write_to_pg(tbl, ALERT_TABLE)

    # ================================================================
    #  规则 vs 模型 —— 这段输出就是 Metabase 对比面板的文字版
    # ================================================================
    both = int((np.array([r[4] for r in rows]) &
                np.array([r[5] for r in rows])).sum())
    only_model = int(sum(1 for r in rows if r[4] and not r[5]))
    only_rule = int(sum(1 for r in rows if r[5] and not r[4]))

    print("\n" + "=" * 66)
    print("  规则 vs 模型")
    print("=" * 66)
    print(f"  两者都报        : {both:>5}")
    print(f"  只有模型报      : {only_model:>5}   ← 规则覆盖不到的时序模式")
    print(f"  只有规则报      : {only_rule:>5}   ← 单点越界但整段演变正常")
    print("\n  这两列的差异正是模型的价值所在：规则看当前这一行，"
          "模型看十分钟的演变。")

    print("\n[score] 模型告警 Top 5:")
    tbl.filter("is_alert = true") \
       .orderBy("anomaly_score", ascending=False) \
       .select("icao24", "window_start", "anomaly_score",
               "top_feature", "rule_alert") \
       .show(5, truncate=False)

    print("=" * 66)
    print(f"  完成。Metabase 里新建问题查 PostgreSQL.{ALERT_TABLE} 即可出图。")
    print("=" * 66)

    spark.stop()


if __name__ == "__main__":
    main()
