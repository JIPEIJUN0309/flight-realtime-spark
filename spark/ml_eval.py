"""
阶段九(4/5)：三方法对照评估 + 阈值选择 + 误报回归测试
========================================================================
在【留出集】上评估：规则阈值 vs Isolation Forest vs LSTM-AE

跑法：
    docker exec -it spark python3 /opt/spark/work-dir/jobs/ml_eval.py

★ 这一步产出的就是要写进报告的那张对照表，以及给 ml_score 用的告警阈值。

评估分三层，一层测召回、一层测误杀、一层做常识校验：

  第一层：合成异常（测召回）
      在留出的正常序列上注入 4 类已知异常，算 PR-AUC。
      测的是"模型能不能把异常捞出来"。

  第二层：真实误报回归（测误杀）★ 本项目最有说服力的一层
      早期把爬升阈值定在 15 m/s，32581 条数据报了 623 条告警（1.9%），
      排查后发现 15 m/s = 2953 ft/min 是客机【正常】的初始爬升率 ——
      那批告警全是误报，后来阈值改到 25 m/s 才降到个位数。
      于是"垂直速率落在 15~25 m/s 之间"的窗口，就是一批
      【经过领域知识确认的、真实的负样本】。
      模型如果把它们也排进异常榜前列，说明它只是复刻了那条坏规则。
      合成异常测得到召回，测不到这个；两边夹住才算数。

  第三层：阈值与去抖动（决定实际误报率）
      重构误差分位数定阈值，再要求连续 N 个窗口超阈值才告警。

★ 为什么用 PR-AUC 而不是 ROC-AUC
  异常检测是极度不平衡问题。ROC 的横轴是 FPR，分母里的负样本极多，
  哪怕误报绝对数量很大，FPR 看起来也很小，曲线会虚高得很好看。
  PR 曲线的横纵轴都只看"被判为正的那部分"，不会被海量负样本稀释。
"""

from __future__ import annotations

import json

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import average_precision_score

import ml_common as mc

POINTS_PATH = "/data/ml/points"
MODEL_DIR = "/data/ml/model"

# 与 ads_export.py 的规则阈值保持一致
TH_RAPID_CLIMB = 25
TH_RAPID_DESCENT = -30
TH_HIGH_ALT = 14500
TH_HIGH_SPEED = 1100
TH_OLD_CLIMB = 15          # 第一版那个报了 623 条误报的阈值

QUANTILES = [0.99, 0.995, 0.999]
DEBOUNCE_GRID = [1, 2, 3]
SEED = 202


# ================================================================
#  基线一：规则阈值
# ================================================================
def rule_score(meta: pd.DataFrame, climb_th: float = TH_RAPID_CLIMB):
    """
    把现有规则表达成一个可排序的连续分：各项违规倍数取最大值。
    >= 1 表示至少触发一条规则，等价于 ads_export.py 里的告警条件。
    """
    return np.maximum.reduce([
        meta["max_vr"].to_numpy() / climb_th,
        meta["min_vr"].to_numpy() / TH_RAPID_DESCENT,
        meta["max_alt"].to_numpy() / TH_HIGH_ALT,
        meta["max_speed"].to_numpy() / TH_HIGH_SPEED,
    ])


# ================================================================
#  基线二：Isolation Forest
# ================================================================
def window_stats(X: np.ndarray) -> np.ndarray:
    """
    把 (n, T, F) 的窗口压成 (n, F*4) 的统计特征。
    ★ 这一步正是 IF 的先天短板：均值/标准差/极值把时间顺序抹掉了，
      "先涨后跌"和"先跌后涨"压出来的统计量完全一样。
      时序依赖是 LSTM 相对它的核心优势，这个对照就是为了让这点可量化。
    """
    return np.concatenate([
        X.mean(axis=1), X.std(axis=1), X.min(axis=1), X.max(axis=1),
    ], axis=1)


# ================================================================
#  阈值 + 去抖动
# ================================================================
def debounce(meta: pd.DataFrame, flag: np.ndarray, n: int) -> np.ndarray:
    """
    要求同一架飞机连续 n 个窗口都超阈值才算告警。
    工业告警系统的标准做法：单点毛刺不报，持续偏离才报。
    """
    if n <= 1:
        return flag.copy()
    out = np.zeros_like(flag)
    order = np.lexsort((meta["window_start"].to_numpy(),
                        meta["icao24"].to_numpy()))
    ac = meta["icao24"].to_numpy()[order]
    f = flag[order]
    run = 0
    for i in range(len(order)):
        if i > 0 and ac[i] != ac[i - 1]:
            run = 0
        run = run + 1 if f[i] else 0
        if run >= n:
            # 连续段达标后，把这一段整体标为告警
            out[order[i - n + 1:i + 1]] = 1
    return out


def main():
    print("=" * 66)
    print("  异常检测 · 三方法对照评估（留出集）")
    print("=" * 66)

    with open(f"{MODEL_DIR}/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    nz = np.load(f"{MODEL_DIR}/norm.npz")
    mu, sd = nz["mu"], nz["sd"]

    import torch
    model = mc.build_model(n_features=cfg["n_features"], hidden=cfg["hidden"],
                           latent=cfg["latent"], seq_len=cfg["seq_len"])
    model.load_state_dict(torch.load(f"{MODEL_DIR}/lstm_ae.pt",
                                     map_location="cpu"))
    print(f"[eval] 载入模型 latent={cfg['latent']}  "
          f"训练时验证 PR-AUC={cfg['val_pr_auc']:.4f}")

    points = pd.read_parquet(POINTS_PATH)
    points["event_time"] = pd.to_datetime(points["event_time"])

    ho = set(cfg["holdout_aircraft"])
    p_ho = points[points["icao24"].isin(ho)]
    p_tr = points[~points["icao24"].isin(ho)]
    print(f"[eval] 留出集 {len(ho)} 架 / {len(p_ho)} 点"
          "（训练与 sweep 全程未见过）")

    # ---- 注入合成异常 ----
    p_inj, intervals = mc.inject_anomalies(p_ho, np.random.default_rng(SEED))
    X, meta = mc.build_windows(p_inj)
    y = mc.label_windows(meta, intervals)
    if len(X) == 0 or y.sum() == 0:
        raise SystemExit("留出集切不出窗口或没注入到异常，先补数据。")
    Xn = mc.apply_normalizer(X, mu, sd)
    print(f"[eval] 留出窗口 {len(X)}，注入异常 {int(y.sum())} 个 "
          f"({y.mean() * 100:.2f}%)")
    print(f"[eval] 注入类型分布: "
          f"{intervals['kind'].value_counts().to_dict()}")

    # ================================================================
    #  三方法打分
    # ================================================================
    s_rule = rule_score(meta)

    Xtr, _ = mc.build_windows(p_tr)
    Xtr_n = mc.apply_normalizer(Xtr, mu, sd)
    iforest = IsolationForest(n_estimators=200, random_state=SEED,
                              contamination="auto")
    iforest.fit(window_stats(Xtr_n))          # 与 AE 用同一批训练数据，公平
    s_if = -iforest.score_samples(window_stats(Xn))

    s_ae, per_feat = mc.reconstruction_error(model, Xn)

    methods = [
        ("规则阈值 (现有 25 m/s)", s_rule),
        ("Isolation Forest", s_if),
        ("LSTM Autoencoder", s_ae),
    ]

    print("\n" + "=" * 66)
    print("  第一层：合成异常检出能力（PR-AUC，越高越好）")
    print("=" * 66)
    print(f"  {'方法':<26}{'PR-AUC':>10}{'相对随机':>12}")
    print("  " + "-" * 48)
    base_rate = y.mean()
    aps = {}
    for name, s in methods:
        ap = average_precision_score(y, s)
        aps[name] = ap
        print(f"  {name:<26}{ap:>10.4f}{ap / base_rate:>11.1f}×")
    print(f"\n  随机猜测的 PR-AUC 等于正样本占比 {base_rate:.4f}，"
          "上表最后一列是相对它的倍数。")

    # ================================================================
    #  第二层：真实误报回归测试
    # ================================================================
    print("\n" + "=" * 66)
    print("  第二层：真实误报回归（越低越好）")
    print("=" * 66)
    # 旧阈值会报、新阈值不报 → 已被航空常识确认为正常爬升
    fp_mask = ((meta["max_vr"].to_numpy() > TH_OLD_CLIMB) &
               (meta["max_vr"].to_numpy() <= TH_RAPID_CLIMB) &
               (y == 0))
    n_fp = int(fp_mask.sum())

    if n_fp == 0:
        print("  留出集里没有落在 15~25 m/s 区间的窗口，本层跳过。")
        print("  （数据量大一些通常就会出现；这批全是巡航段。）")
    else:
        print(f"  确认为正常爬升的窗口: {n_fp} 个"
              f"（占留出集 {n_fp / len(meta) * 100:.2f}%）")
        print(f"\n  {'方法':<26}{'落入 top-K 比例':>16}")
        print("  " + "-" * 44)
        K = max(int(len(meta) * 0.01), 1)      # top 1% 视为"会被送到人眼前"
        for name, s in methods:
            topk = np.zeros(len(s), dtype=bool)
            topk[np.argsort(-s)[:K]] = True
            hit = topk[fp_mask].mean()
            print(f"  {name:<26}{hit * 100:>15.1f}%")
        print(f"\n  基准：随机排序下应为 {K / len(meta) * 100:.1f}%。")
        print("  规则基线在这一层必然很高——这批窗口本来就是它报出来的。")
        print("  模型如果也很高，说明它只是学会了同一条坏规则。")

    # ================================================================
    #  第三层：阈值与去抖动
    # ================================================================
    print("\n" + "=" * 66)
    print("  第三层：阈值分位数 × 连续窗口去抖动（LSTM-AE）")
    print("=" * 66)
    print(f"  {'分位数':>8}{'阈值':>12}{'N':>4}{'召回':>9}{'误报率':>10}{'告警数':>9}")
    print("  " + "-" * 54)

    rows = []
    for q in QUANTILES:
        th = float(np.quantile(s_ae, q))
        flag = (s_ae >= th).astype(int)
        for n in DEBOUNCE_GRID:
            fired = debounce(meta, flag, n)
            tp = int(((fired == 1) & (y == 1)).sum())
            fp = int(((fired == 1) & (y == 0)).sum())
            recall = tp / max(int(y.sum()), 1)
            fpr = fp / max(int((y == 0).sum()), 1)
            rows.append({"q": q, "threshold": th, "debounce": n,
                         "recall": recall, "fpr": fpr,
                         "n_alert": int(fired.sum())})
            print(f"  {q:>8.3f}{th:>12.5f}{n:>4}{recall:>8.1%}"
                  f"{fpr:>10.2%}{int(fired.sum()):>9}")

    # 选工作点：召回不低于 0.6 的前提下误报率最低
    ok = [r for r in rows if r["recall"] >= 0.6]
    pick = min(ok or rows, key=lambda r: r["fpr"])
    print(f"\n  选定工作点: 分位数 {pick['q']}, 连续 {pick['debounce']} 窗口"
          f" → 召回 {pick['recall']:.1%}, 误报率 {pick['fpr']:.2%}")
    if not ok:
        print("  （没有任何配置达到 60% 召回，说明数据量偏小，"
              "这个工作点只是当前数据下的最优解）")

    # ================================================================
    #  归因：异常主要来自哪个特征
    # ================================================================
    print("\n" + "=" * 66)
    print("  归因：高分窗口的重构误差来自哪些特征")
    print("=" * 66)
    top_idx = np.argsort(-s_ae)[:max(int(len(s_ae) * 0.01), 1)]
    share = per_feat[top_idx].mean(axis=0)
    share = share / share.sum()
    for name, v in sorted(zip(mc.FEATURES, share), key=lambda x: -x[1])[:5]:
        bar = "█" * int(v * 40)
        print(f"  {name:<18}{v * 100:>6.1f}%  {bar}")
    print("\n  这一列就是告警里的'原因'字段——从一个分数变成一条可查的线索。")

    # ---- 回写阈值，供 ml_score 使用 ----
    cfg["threshold"] = pick["threshold"]
    cfg["threshold_quantile"] = pick["q"]
    cfg["debounce"] = pick["debounce"]
    cfg["eval"] = {
        "pr_auc": {k: float(v) for k, v in aps.items()},
        "base_rate": float(base_rate),
        "n_holdout_windows": int(len(meta)),
        "n_injected": int(y.sum()),
        "recall": pick["recall"],
        "fpr": pick["fpr"],
    }
    with open(f"{MODEL_DIR}/config.json", "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 66)
    print("  评估结果已写回 config.json（ml_score.py 会读里面的阈值）")
    print("  下一步： docker exec -it spark /opt/spark/bin/spark-submit \\")
    print("             /opt/spark/work-dir/jobs/ml_score.py")
    print("=" * 66)


if __name__ == "__main__":
    main()
