"""
阶段九(3/5)：训练 LSTM Autoencoder + 瓶颈维度 sweep
========================================================================
读航迹序列 → 切滑窗 → 选瓶颈维度 → 训练最终模型 → 存权重

跑法（这一步不需要 Spark，直接用容器里的 python）：
    docker exec -it spark python3 /opt/spark/work-dir/jobs/ml_train.py

★ 为什么用 Autoencoder 而不是分类器

  没有标签。分类器要有标注好的正负样本，而真实异常既稀少又无人标注。
  AE 是自监督的，只依赖一个假设：【绝大多数数据是正常的】。
  它在正常模式上学出一个低维表示，异常序列不在这个流形上、重构不回来，
  重构误差自然就大 —— 误差即异常分。

★ 为什么按【飞机】划分数据集，而不是按窗口

  滑窗步长 5、窗长 20，相邻窗口重叠 75%。如果随机按窗口切分，
  同一架飞机的几乎相同的窗口会同时出现在训练集和验证集里，
  验证分数会虚高一大截。这是时序建模最常见的泄漏方式。

★ 瓶颈维度为什么要 sweep，而不是拍一个 8

  这是 AE 异常检测最主要的失效模式：
    latent 太大 → AE 退化成恒等映射，把异常也原样重构出来，异常分失去区分度
    latent 太小 → 连正常序列都还原不了，所有样本误差都大，同样没区分度
  而且【验证集重构损失越低越好】在这里是错的 —— 损失低可能正是过拟合的信号。
  所以 sweep 的评判标准不是重构损失，是【区分能力】：
  在验证集上注入合成异常，看哪个维度的 PR-AUC 最高。
"""

from __future__ import annotations

import json
import os

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import average_precision_score

import ml_common as mc

POINTS_PATH = "/data/ml/points"
MODEL_DIR = "/data/ml/model"

LATENT_GRID = [4, 8, 16, 32]     # 瓶颈维度候选
HIDDEN = 32
SWEEP_EPOCHS = 25                # sweep 阶段跑短一点，只用来比较
FINAL_EPOCHS = 80
PATIENCE = 8
BATCH = 128
LR = 1e-3
SEED = 42


# ================================================================
#  训练
# ================================================================
def train_once(Xtr, Xva, latent, epochs, seed=SEED, verbose=False):
    """训练一个 AE，返回 (model, best_val_loss)。带早停。"""
    torch.manual_seed(seed)
    np.random.seed(seed)

    model = mc.build_model(n_features=Xtr.shape[-1], hidden=HIDDEN,
                           latent=latent, seq_len=Xtr.shape[1])
    opt = torch.optim.Adam(model.parameters(), lr=LR)
    lossf = torch.nn.MSELoss()

    tr = torch.tensor(Xtr, dtype=torch.float32)
    va = torch.tensor(Xva, dtype=torch.float32)

    best, best_state, bad = float("inf"), None, 0
    for ep in range(epochs):
        model.train()
        perm = torch.randperm(len(tr))
        tot = 0.0
        for i in range(0, len(tr), BATCH):
            xb = tr[perm[i:i + BATCH]]
            opt.zero_grad()
            loss = lossf(model(xb), xb)
            loss.backward()
            # 梯度裁剪：LSTM 长序列训练容易梯度爆炸
            torch.nn.utils.clip_grad_norm_(model.parameters(), 5.0)
            opt.step()
            tot += loss.item() * len(xb)
        tr_loss = tot / len(tr)

        model.eval()
        with torch.no_grad():
            va_loss = lossf(model(va), va).item()

        if verbose and (ep % 10 == 0 or ep == epochs - 1):
            print(f"      epoch {ep:>3}  train {tr_loss:.5f}  val {va_loss:.5f}")

        if va_loss < best - 1e-5:
            best, bad = va_loss, 0
            best_state = {k: v.clone() for k, v in model.state_dict().items()}
        else:
            bad += 1
            if bad >= PATIENCE:
                if verbose:
                    print(f"      早停于 epoch {ep}")
                break

    if best_state is not None:
        model.load_state_dict(best_state)
    return model, best


def main():
    print("=" * 62)
    print("  异常检测 · 训练 LSTM Autoencoder")
    print("=" * 62)

    # ---- 1) 读点表 ----
    points = pd.read_parquet(POINTS_PATH)
    points["event_time"] = pd.to_datetime(points["event_time"])
    aircraft = np.array(sorted(points["icao24"].unique()))
    print(f"[train] 航迹点 {len(points)} 条 / 飞机 {len(aircraft)} 架")

    if len(aircraft) < 30:
        print("\n  ⚠️  飞机数太少，训练出来的'正常模式'不具代表性。")
        print("      先把 opensky_producer.py 连续挂 6-12 小时再回来。\n")

    # ---- 2) 按飞机划分 train / val / holdout ----
    rng = np.random.default_rng(SEED)
    rng.shuffle(aircraft)
    n = len(aircraft)
    n_tr, n_va = int(n * 0.6), int(n * 0.2)
    ac_tr = set(aircraft[:n_tr])
    ac_va = set(aircraft[n_tr:n_tr + n_va])
    ac_ho = set(aircraft[n_tr + n_va:])
    print(f"[train] 划分（按飞机，避免窗口重叠导致的泄漏）: "
          f"训练 {len(ac_tr)} / 验证 {len(ac_va)} / 留出 {len(ac_ho)}")

    p_tr = points[points["icao24"].isin(ac_tr)]
    p_va = points[points["icao24"].isin(ac_va)]

    Xtr, _ = mc.build_windows(p_tr)
    print(f"[train] 训练窗口: {len(Xtr)}")
    if len(Xtr) < 200:
        print("\n  ⚠️  训练窗口不足 200 个，结果只能当流程验证，不要写进报告。\n")

    # ---- 3) 标准化（★ 统计量只用训练集算）----
    mu, sd = mc.fit_normalizer(Xtr)
    Xtr_n = mc.apply_normalizer(Xtr, mu, sd)

    # ---- 4) 验证集：原样 + 注入合成异常，用来评判区分能力 ----
    p_va_inj, intervals = mc.inject_anomalies(p_va, np.random.default_rng(SEED + 1))
    Xva, meta_va = mc.build_windows(p_va_inj)
    y_va = mc.label_windows(meta_va, intervals)
    Xva_n = mc.apply_normalizer(Xva, mu, sd)
    print(f"[train] 验证窗口: {len(Xva)}，其中注入异常 {int(y_va.sum())} 个 "
          f"({y_va.mean() * 100:.1f}%)")

    if y_va.sum() == 0:
        raise SystemExit("验证集没有注入到异常，样本量太小，先补数据再训练。")

    # ================================================================
    #  5) 瓶颈维度 sweep
    #     判据是 PR-AUC（区分能力），不是验证集重构损失
    # ================================================================
    print("\n[train] 瓶颈维度 sweep（判据：注入异常的 PR-AUC）")
    print(f"      {'latent':>7} {'val_loss':>10} {'PR-AUC':>9}")
    print("      " + "-" * 28)

    results = []
    for latent in LATENT_GRID:
        model, val_loss = train_once(Xtr_n, Xva_n, latent, SWEEP_EPOCHS)
        score, _ = mc.reconstruction_error(model, Xva_n)
        ap = average_precision_score(y_va, score)
        results.append({"latent": latent, "val_loss": val_loss, "pr_auc": ap})
        print(f"      {latent:>7} {val_loss:>10.5f} {ap:>9.4f}")

    best = max(results, key=lambda r: r["pr_auc"])
    best_latent = best["latent"]
    print(f"\n[train] 选定 latent = {best_latent}（PR-AUC {best['pr_auc']:.4f}）")

    # 打印 sweep 现象：重构损失最低的维度，区分度不一定最好
    lo = min(results, key=lambda r: r["val_loss"])
    if lo["latent"] != best_latent:
        print(f"      注意：重构损失最低的是 latent={lo['latent']}"
              f"（{lo['val_loss']:.5f}），但它的 PR-AUC 只有 {lo['pr_auc']:.4f}。")
        print("      这正是瓶颈过宽的表现——AE 把异常也重构得很好，"
              "损失漂亮但区分不开。")

    # ---- 6) 用选定维度训练最终模型 ----
    print(f"\n[train] 训练最终模型（latent={best_latent}, "
          f"最多 {FINAL_EPOCHS} 轮，早停 patience={PATIENCE}）")
    model, final_val = train_once(Xtr_n, Xva_n, best_latent,
                                  FINAL_EPOCHS, verbose=True)
    score, _ = mc.reconstruction_error(model, Xva_n)
    final_ap = average_precision_score(y_va, score)
    print(f"[train] 最终模型 val_loss={final_val:.5f}  PR-AUC={final_ap:.4f}")

    # ---- 7) 存盘 ----
    os.makedirs(MODEL_DIR, exist_ok=True)
    torch.save(model.state_dict(), f"{MODEL_DIR}/lstm_ae.pt")
    np.savez(f"{MODEL_DIR}/norm.npz", mu=mu, sd=sd)
    config = {
        "latent": best_latent,
        "hidden": HIDDEN,
        "seq_len": mc.SEQ_LEN,
        "stride": mc.STRIDE,
        "features": mc.FEATURES,
        "n_features": mc.N_FEATURES,
        "val_pr_auc": float(final_ap),
        "sweep": results,
        "holdout_aircraft": sorted(ac_ho),
    }
    with open(f"{MODEL_DIR}/config.json", "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"\n[train] 已保存到 {MODEL_DIR}/")
    print("        lstm_ae.pt / norm.npz / config.json")
    print("=" * 62)
    print("  下一步： docker exec -it spark python3 "
          "/opt/spark/work-dir/jobs/ml_eval.py")
    print("=" * 62)


if __name__ == "__main__":
    main()
