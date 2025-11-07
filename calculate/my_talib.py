from typing import Any

import numpy as np
import pandas as pd
import talib
from numpy.typing import NDArray


def to_ndarray(x: Any) -> NDArray[np.float64]:
    # 保证传入 talib 的是 np.ndarray[float64]
    return np.asarray(x, dtype=np.float64)


def calculate_ma(data: pd.DataFrame) -> pd.DataFrame:
    close: NDArray[np.float64] = to_ndarray(data["close"])
    return pd.DataFrame(
        {
            "ma10": talib.MA(close, 10),
            "ma20": talib.MA(close, 20),
            "ma60": talib.MA(close, 60),
        },
        index=data.index,
    )


def calculate_ma_power_ratio(
    ma_df: pd.DataFrame, slope_window: int = 5
) -> pd.DataFrame:
    """
    计算 MA 多头排列强度指标及其斜率（捕捉趋势发散加速度）

    参数：
        ma_df: 每列是一根 MA，索引为日期
        slope_window: 计算斜率的滚动窗口（默认为5）

    返回：
        DataFrame，包含:
            - ma_power_ratio: 多头排列强度（0~1，越大越多头）
            - ma_power_slope: 强度变化速度（越大说明发散加快）
    """
    ma_np = ma_df.values  # (T, m)
    T, m = ma_np.shape
    ma_max = max(int(col.replace("ma", "")) for col in ma_df.columns)

    # 上三角索引组合 (i < j)
    triu_i, triu_j = np.triu_indices(m, k=1)
    left = ma_np[:, triu_i]  # (T, num_pairs)
    right = ma_np[:, triu_j]  # (T, num_pairs)

    # 多头排列比例
    bull_counts = np.sum(left > right, axis=1)
    max_inv = m * (m - 1) / 2.0
    ratio = bull_counts / max_inv

    # 前 ma_max 个值设为 NaN
    ratio[:ma_max] = np.nan

    # 计算斜率：简单差分或线性回归近似
    slope = np.full_like(ratio, np.nan)
    valid_idx = ~np.isnan(ratio)
    valid_ratio = ratio[valid_idx]
    if len(valid_ratio) > slope_window:
        # 用 rolling window 差分近似斜率
        slope_series = pd.Series(valid_ratio).diff(slope_window) / slope_window
        slope[valid_idx] = slope_series.values

    # 返回结果
    return pd.DataFrame(
        {"ma_power_ratio": ratio, "ma_power_slope": slope}, index=ma_df.index
    )


def calculate_mavol(data: pd.DataFrame) -> pd.DataFrame:
    vol: NDArray[np.float64] = to_ndarray(data["volume"])
    return pd.DataFrame(
        {
            "ma5_vol": talib.MA(vol, 5),
            "ma10_vol": talib.MA(vol, 10),
            "ma20_vol": talib.MA(vol, 20),
        },
        index=data.index,
    )


def calculate_macd(data: pd.DataFrame) -> pd.DataFrame:
    close: NDArray[np.float64] = to_ndarray(data["close"])
    macd, signal, hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
    return pd.DataFrame(
        {
            "macd": macd,
            "signal": signal,
            "hist": hist * 2,
        },
        index=data.index,
    )


def calculate_adx(data: pd.DataFrame) -> pd.DataFrame:
    high: NDArray[np.float64] = to_ndarray(data["high"])
    low: NDArray[np.float64] = to_ndarray(data["low"])
    close: NDArray[np.float64] = to_ndarray(data["close"])
    return pd.DataFrame(
        {
            "adx": talib.ADX(high, low, close, timeperiod=14),
            "pdi": talib.PLUS_DI(high, low, close, timeperiod=14),
            "mdi": talib.MINUS_DI(high, low, close, timeperiod=14),
        },
        index=data.index,
    )


def calculate_atr(data: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    high: NDArray[np.float64] = to_ndarray(data["high"])
    low: NDArray[np.float64] = to_ndarray(data["low"])
    close: NDArray[np.float64] = to_ndarray(data["close"])

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]  # 避免第一个变为随机值或过大误差

    tr = np.maximum.reduce(
        [high - low, np.abs(high - prev_close), np.abs(low - prev_close)]
    )

    atr = pd.Series(tr, index=data.index).rolling(period, min_periods=1).mean()

    return pd.DataFrame({"atr": atr}, index=data.index)


def calculate_bbands(data: pd.DataFrame) -> pd.DataFrame:
    close: NDArray[np.float64] = to_ndarray(data["close"])
    upper, middle, lower = talib.BBANDS(close, timeperiod=20, nbdevup=2, nbdevdn=2)
    width = (upper - lower) / middle
    return pd.DataFrame(
        {
            "bb_upper": upper,
            "bb_middle": middle,
            "bb_lower": lower,
            "bb_width": width,
        },
        index=data.index,
    )
