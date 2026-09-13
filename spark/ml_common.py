"""
阶段九(1/5)：异常检测公共模块 —— 特征派生 / 滑窗 / 模型定义
========================================================================
本模块不单独运行，被 ml_train / ml_eval / ml_score 三个作业共同引用，
保证"训练时怎么造特征"和"打分时怎么造特征"是同一份代码。

★ 为什么派生特征放在 pandas 而不是 Spark SQL？
  这一层要被三个地方复用，其中 ml_eval 还要在"注入合成异常之后重新派生"。
  如果 Spark SQL 写一份、pandas 再写一份，两份逻辑迟早会漂移，
  线上打分和离线评估算出来的特征就对不上了（训练/服务偏斜，
  training-serving skew，是特征工程最典型的事故）。
  所以派生只留一份，放在这里；Spark 负责它真正擅长的事：
  从 Delta 湖里把明细抽出来、按飞机做连续性筛选（见 ml_features.py）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# ================================================================
#  滑窗与建模参数
# ================================================================
SEQ_LEN = 20          # 每个样本 20 个采样点。采集间隔 30s → 10 分钟
                      # 定 10 分钟是因为爬升、转弯这类机动的典型持续时间
                      # 就在几分钟量级；窗口太短看不到"演变"，太长会把
                      # 一次异常稀释在大量正常点里。
STRIDE = 5            # 滑动步长，窗口间重叠 75%，提高样本利用率
MIN_POINTS = 30       # 一架飞机少于 30 个点就不够切窗口，直接排除
RESAMPLE_SEC = 30     # 采集间隔基准，用来算 gap_ratio

# 模型输入的 8 个特征。顺序固定，训练和打分都按这个顺序。
FEATURES = [
    "altitude_m",       # 气压高度 (m)
    "speed_ms",         # 地速 (m/s)
    "vertical_rate",    # 垂直速率 (m/s)
    "accel",            # 加速度 (m/s²)     —— 速度阶跃
    "vr_delta",         # 垂直速率变化率     —— 爬升率震荡（规则看不见的那类）
    "turn_rate",        # 转弯率 (deg/s)     —— 航向抖动
    "pos_speed_resid",  # 位置推算速度残差    —— 定位漂移
    "gap_ratio",        # 采样间隔 / 30s      —— 缺报 / 信号中断
]
N_FEATURES = len(FEATURES)

# 从 silver 表里取的原始列（注入合成异常时改的就是这几列）
RAW_COLS = [
    "icao24", "event_time",
    "latitude", "longitude",
    "altitude_m", "speed_kmh", "heading_deg", "vertical_rate",
]

EARTH_R_M = 6_371_000.0


# ================================================================
#  几何工具
# ================================================================
def haversine_m(lat1, lon1, lat2, lon2):
    """球面两点距离(米)。和 dwd_silver.py 里那段 Spark SQL 是同一个公式。"""
    p1 = np.radians(lat1)
    p2 = np.radians(lat2)
    dp = p2 - p1
    dl = np.radians(np.asarray(lon2) - np.asarray(lon1))
    a = np.sin(dp / 2.0) ** 2 + np.cos(p1) * np.cos(p2) * np.sin(dl / 2.0) ** 2
    return 2.0 * EARTH_R_M * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))


def circular_delta(cur, prev):
    """
    ★ 航向角是 0–360 的环形量，必须做环向差值。

    359° → 1° 的真实变化是 +2 度，直接相减会得到 -358 度。
    模型会把每一次穿越正北都当成剧烈转弯，这是这类数据最典型的坑。
    归一到 (-180, 180] 之后才是真实转角。
    """
    d = np.asarray(cur, dtype=float) - np.asarray(prev, dtype=float)
    return (d + 180.0) % 360.0 - 180.0


# ================================================================
#  特征派生
# ================================================================
def derive_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    输入：单架飞机的原始点序列（含 RAW_COLS），按时间升序
    输出：同样长度的 DataFrame，多出 FEATURES 里的派生列

    首行没有前一点，派生量为 NaN，由调用方在切窗口时跳过。
    """
    df = df.sort_values("event_time").reset_index(drop=True).copy()

    # 采样间隔（秒）。时间戳倒挂或重复点会得到 <=0，置为 NaN 丢弃。
    t_sec = pd.to_datetime(df["event_time"]).astype("int64").to_numpy() / 1e9
    dt = np.diff(t_sec, prepend=np.nan)
    dt = np.where(dt > 0, dt, np.nan)

    speed_ms = df["speed_kmh"].to_numpy(dtype=float) / 3.6
    alt = df["altitude_m"].to_numpy(dtype=float)
    vr = df["vertical_rate"].to_numpy(dtype=float)
    hdg = df["heading_deg"].to_numpy(dtype=float)
    lat = df["latitude"].to_numpy(dtype=float)
    lon = df["longitude"].to_numpy(dtype=float)

    prev = lambda a: np.concatenate(([np.nan], a[:-1]))

    # 加速度：速度阶跃（信号丢失后重连最典型）
    accel = (speed_ms - prev(speed_ms)) / dt
    # 垂直速率的变化率：抓"爬升率震荡"，单点阈值规则完全看不到这个
    vr_delta = (vr - prev(vr)) / dt
    # 转弯率：环向差值后再除以间隔
    turn_rate = circular_delta(hdg, prev(hdg)) / dt

    # 位置推算速度 vs 上报地速的残差。
    # 正常情况下两者应该接近；定位漂移会让位置跳一下而地速不变，
    # 残差就会突然变大 —— 单看任何一个字段都发现不了。
    step_m = haversine_m(prev(lat), prev(lon), lat, lon)
    pos_speed = step_m / dt
    pos_speed_resid = np.abs(pos_speed - speed_ms)

    # 采样间隔比值：正常 ≈ 1；设备缺报会让它跳到 2、3、4……
    gap_ratio = dt / RESAMPLE_SEC

    df["speed_ms"] = speed_ms
    df["accel"] = accel
    df["vr_delta"] = vr_delta
    df["turn_rate"] = turn_rate
    df["pos_speed_resid"] = pos_speed_resid
    df["gap_ratio"] = gap_ratio
    return df


def build_windows(points: pd.DataFrame,
                  seq_len: int = SEQ_LEN,
                  stride: int = STRIDE):
    """
    原始点表 → 滑窗样本。

    返回
      X    : ndarray (n_windows, seq_len, N_FEATURES)   未标准化
      meta : DataFrame  每个窗口一行：icao24 / window_start / window_end
                        以及规则基线要用的 max_vr / min_vr / max_alt / max_speed
    """
    xs, metas = [], []

    for icao, g in points.groupby("icao24", sort=False):
        if len(g) < seq_len + 1:
            continue
        g = derive_features(g)
        feat = g[FEATURES].to_numpy(dtype=float)
        times = pd.to_datetime(g["event_time"]).to_numpy()
        vr = g["vertical_rate"].to_numpy(dtype=float)
        alt = g["altitude_m"].to_numpy(dtype=float)
        spd = g["speed_kmh"].to_numpy(dtype=float)

        # 第 0 行的派生量必为 NaN，从 1 开始切
        for s in range(1, len(g) - seq_len + 1, stride):
            e = s + seq_len
            block = feat[s:e]
            if not np.isfinite(block).all():
                continue          # 窗口内有缺失就整窗丢弃，不插值
            xs.append(block)
            metas.append({
                "icao24": icao,
                "window_start": times[s],
                "window_end": times[e - 1],
                # 下面四个只给规则基线用，不进模型
                "max_vr": float(np.nanmax(vr[s:e])),
                "min_vr": float(np.nanmin(vr[s:e])),
                "max_alt": float(np.nanmax(alt[s:e])),
                "max_speed": float(np.nanmax(spd[s:e])),
            })

    if not xs:
        return np.empty((0, seq_len, N_FEATURES)), pd.DataFrame()
    return np.stack(xs), pd.DataFrame(metas)


# ================================================================
#  合成异常注入
# ================================================================
#  ★ 为什么要注入，以及它和"编造问题"的区别
#
#  真实的航迹异常标签不存在 —— 没有任何公开的 ADS-B 数据集带异常标注，
#  工业设备的振动/温度时序同样如此：正常数据海量，异常样本极少且无人标注。
#  在标签本质上不可得的前提下，"往正常序列里注入已知形态的异常、
#  再看模型能不能捞回来"是这个领域论文和工程实践通用的评估手段。
#
#  它评估的是【检测能力】，不是在假装遇到过真实故障。
#  所以本项目的评估不止这一层：ml_eval.py 里还有一组【真实的】负样本
#  —— 早期 15 m/s 阈值报出、后来被航空常识判定为正常爬升的那批航迹。
#  合成异常测召回，真实误报测误杀，两边夹住才算数。
# ----------------------------------------------------------------
INJECT_KINDS = ("ALT_STEP", "SPEED_DROP", "POS_DRIFT", "DROPOUT")


def inject_anomalies(points: pd.DataFrame, rng: np.random.Generator,
                     rate: float = 0.35):
    """
    在原始点序列上注入合成异常（改的是 RAW_COLS，派生特征随后重算，
    这样异常会自然地传导到 accel / turn_rate / gap_ratio 等派生量上，
    而不是人为地去改派生列 —— 后者等于直接把答案喂给模型）。

    参数
      rate : 每架飞机被注入的概率

    返回
      modified  : 注入后的点表
      intervals : DataFrame[icao24, t_start, t_end, kind]  异常时间区间
    """
    out_frames, intervals = [], []

    for icao, g in points.groupby("icao24", sort=False):
        g = g.sort_values("event_time").reset_index(drop=True).copy()
        n = len(g)
        if n < SEQ_LEN + 6 or rng.random() > rate:
            out_frames.append(g)
            continue

        kind = INJECT_KINDS[rng.integers(len(INJECT_KINDS))]
        i = int(rng.integers(2, n - 6))          # 留出首尾余量
        t = pd.to_datetime(g["event_time"])

        if kind == "ALT_STEP":
            # 传感器跳变：高度突增 500m 并持续 3 个点
            g.loc[i:i + 2, "altitude_m"] = g.loc[i:i + 2, "altitude_m"] + 500.0
            t0, t1 = t.iloc[i], t.iloc[i + 2]

        elif kind == "SPEED_DROP":
            # 信号丢失再重连：地速骤降 40% 后恢复
            g.loc[i:i + 2, "speed_kmh"] = g.loc[i:i + 2, "speed_kmh"] * 0.6
            t0, t1 = t.iloc[i], t.iloc[i + 2]

        elif kind == "POS_DRIFT":
            # 定位漂移：经纬度叠加随机游走（量级约几公里）
            k = 4
            walk = np.cumsum(rng.normal(0, 0.010, size=k))
            g.loc[i:i + k - 1, "latitude"] = g.loc[i:i + k - 1, "latitude"] + walk
            g.loc[i:i + k - 1, "longitude"] = g.loc[i:i + k - 1, "longitude"] + walk * 0.6
            t0, t1 = t.iloc[i], t.iloc[i + k - 1]

        else:  # DROPOUT
            # 设备缺报：连续 4 个采样点消失，制造一段时间空洞。
            # 区间取 [前一点, 恢复点]，因为异常体现在 gap_ratio 的跳变上。
            k = 4
            t0, t1 = t.iloc[i - 1], t.iloc[min(i + k, n - 1)]
            g = g.drop(index=range(i, min(i + k, n))).reset_index(drop=True)

        intervals.append({"icao24": icao, "t_start": t0, "t_end": t1,
                          "kind": kind})
        out_frames.append(g)

    modified = pd.concat(out_frames, ignore_index=True)
    return modified, pd.DataFrame(intervals)


def label_windows(meta: pd.DataFrame, intervals: pd.DataFrame) -> np.ndarray:
    """窗口时间范围与任一注入区间有重叠 → 标为异常(1)。"""
    y = np.zeros(len(meta), dtype=int)
    if meta.empty or intervals.empty:
        return y

    by_ac = {k: v for k, v in intervals.groupby("icao24")}
    ws = pd.to_datetime(meta["window_start"]).to_numpy()
    we = pd.to_datetime(meta["window_end"]).to_numpy()

    for idx, icao in enumerate(meta["icao24"].to_numpy()):
        iv = by_ac.get(icao)
        if iv is None:
            continue
        a = pd.to_datetime(iv["t_start"]).to_numpy()
        b = pd.to_datetime(iv["t_end"]).to_numpy()
        # 区间重叠判定：窗口起点 <= 区间终点 且 窗口终点 >= 区间起点
        if np.any((ws[idx] <= b) & (we[idx] >= a)):
            y[idx] = 1
    return y


# ================================================================
#  标准化
# ================================================================
def fit_normalizer(X: np.ndarray):
    """逐特征算均值和标准差。★ 只能用训练集算，否则信息泄漏。"""
    flat = X.reshape(-1, X.shape[-1])
    mu = flat.mean(axis=0)
    sd = flat.std(axis=0)
    sd = np.where(sd < 1e-8, 1.0, sd)     # 防止常量特征除零
    return mu, sd


def apply_normalizer(X: np.ndarray, mu: np.ndarray, sd: np.ndarray):
    return (X - mu) / sd


# ================================================================
#  LSTM Autoencoder
# ================================================================
def build_model(n_features: int = N_FEATURES,
                hidden: int = 32,
                latent: int = 8,
                seq_len: int = SEQ_LEN):
    """
    延迟 import torch：ml_features.py 这类不需要模型的作业就不用装 torch 也能跑。
    """
    import torch
    import torch.nn as nn

    class LSTMAutoencoder(nn.Module):
        """
        Encoder: LSTM(n_feat→hidden) → LSTM(hidden→latent)，取末隐状态作为 z
        Decoder: z 复制 seq_len 份 → LSTM(latent→hidden) → LSTM(hidden→n_feat)

        ★ latent（瓶颈）维度是这个方法最关键的超参，ml_train.py 会做 sweep：
          给太宽 → AE 退化成恒等映射，连异常也能原样重构出来，异常分失去区分度；
          给太窄 → 连正常序列都还原不了，所有样本误差都大，同样没区分度。
        """

        def __init__(self):
            super().__init__()
            self.seq_len = seq_len
            self.enc1 = nn.LSTM(n_features, hidden, batch_first=True)
            self.enc2 = nn.LSTM(hidden, latent, batch_first=True)
            self.dec1 = nn.LSTM(latent, hidden, batch_first=True)
            self.dec2 = nn.LSTM(hidden, n_features, batch_first=True)

        def forward(self, x):
            h, _ = self.enc1(x)
            _, (hn, _) = self.enc2(h)
            z = hn[-1]                                   # (B, latent)
            r = z.unsqueeze(1).expand(-1, self.seq_len, -1)
            d, _ = self.dec1(r)
            out, _ = self.dec2(d)
            return out

    return LSTMAutoencoder()


def reconstruction_error(model, X: np.ndarray, batch: int = 256):
    """
    返回
      score    : (n,)                每个窗口的重构 MSE —— 异常分
      per_feat : (n, N_FEATURES)     逐特征的 MSE —— 用来做归因
                                     （"这次异常 72% 来自 vertical_rate"）
    """
    import torch

    model.eval()
    scores, per_feats = [], []
    with torch.no_grad():
        for i in range(0, len(X), batch):
            xb = torch.tensor(X[i:i + batch], dtype=torch.float32)
            out = model(xb)
            err = (out - xb) ** 2                 # (B, T, F)
            per_feats.append(err.mean(dim=1).numpy())
            scores.append(err.mean(dim=(1, 2)).numpy())
    return np.concatenate(scores), np.concatenate(per_feats)
