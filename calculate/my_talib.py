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


def calculate_heikin_ashi(data: pd.DataFrame) -> pd.DataFrame:
    ha_close = (data["open"] + data["high"] + data["low"] + data["close"]) / 4

    ha_open = ha_close.copy()
    ha_open.iloc[0] = data["open"].iloc[0]
    ha_open = (ha_open.shift(1) + ha_open) / 2
    ha_open.iloc[0] = data["open"].iloc[0]

    ha_high = np.maximum.reduce([ha_open, ha_close, data["high"]])
    ha_low = np.minimum.reduce([ha_open, ha_close, data["low"]])

    return pd.DataFrame(
        {
            "ha_open": ha_open,
            "ha_close": ha_close,
            "ha_high": ha_high,
            "ha_low": ha_low,
            "ha_color": (ha_close > ha_open).astype(int),
        },
        index=data.index,
    )


def calculate_zigzag(
    df: pd.DataFrame,
    pct: float = None,
    abs_thresh: float = None,
    atr_mult: float = 2,
    atr_period: int = 14,
    min_bars: int = 3,
) -> pd.DataFrame:
    """
    基于 High/Low/ATR 的 ZigZag 算法，返回带 ZigZag 列的 DataFrame

    参数:
        df: 必须包含 ['High','Low','Close']
        pct: 百分比阈值
        abs_thresh: 绝对价格阈值
        atr_mult: ATR 倍数阈值
        atr_period: ATR 计算周期
        min_bars: 两个拐点之间至少间隔多少根K线

    返回:
        df: 增加 pivot, zigzagpoint, zigzag 三列
    """
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    n = len(df)

    # 计算 ATR
    if atr_mult is not None:
        df = calculate_atr(df, atr_period)
    else:
        df["atr"] = np.nan

    pivots = np.zeros(n, dtype=int)
    points = np.full(n, np.nan)

    # 阈值检查函数
    def moved_up(ref_price, cur_price, i):
        if pct is not None:
            return cur_price >= ref_price * (1 + pct)
        elif abs_thresh is not None:
            return (cur_price - ref_price) >= abs_thresh
        elif atr_mult is not None:
            return (cur_price - ref_price) >= atr_mult * df["atr"].iloc[i]
        else:
            raise ValueError("必须提供 pct、abs_thresh 或 atr_mult 之一")

    def moved_down(ref_price, cur_price, i):
        if pct is not None:
            return cur_price <= ref_price * (1 - pct)
        elif abs_thresh is not None:
            return (ref_price - cur_price) >= abs_thresh
        elif atr_mult is not None:
            return (ref_price - cur_price) >= atr_mult * df["atr"].iloc[i]
        else:
            raise ValueError("必须提供 pct、abs_thresh 或 atr_mult 之一")

    # 初始化
    last_ext_idx = 0
    last_ext_price = close[0]
    last_type = None  # 'peak' / 'valley'

    for i in range(1, n):
        # 用 high/low 判断
        p_high, p_low = high[i], low[i]

        if last_type is None:
            # 确认初始方向
            if moved_up(last_ext_price, p_high, i) and (i - last_ext_idx) >= min_bars:
                pivots[i] = 1
                points[i] = p_high
                last_ext_idx, last_ext_price, last_type = i, p_high, "peak"
            elif (
                moved_down(last_ext_price, p_low, i) and (i - last_ext_idx) >= min_bars
            ):
                pivots[i] = -1
                points[i] = p_low
                last_ext_idx, last_ext_price, last_type = i, p_low, "valley"

        elif last_type == "peak":
            # 如果有更高点，替换峰
            if p_high >= last_ext_price:
                pivots[last_ext_idx] = 0
                points[last_ext_idx] = np.nan
                pivots[i] = 1
                points[i] = p_high
                last_ext_idx, last_ext_price = i, p_high
            # 检查是否跌破阈值，形成 valley
            elif (
                moved_down(last_ext_price, p_low, i) and (i - last_ext_idx) >= min_bars
            ):
                pivots[i] = -1
                points[i] = p_low
                last_ext_idx, last_ext_price, last_type = i, p_low, "valley"

        elif last_type == "valley":
            # 如果有更低点，替换谷
            if p_low <= last_ext_price:
                pivots[last_ext_idx] = 0
                points[last_ext_idx] = np.nan
                pivots[i] = -1
                points[i] = p_low
                last_ext_idx, last_ext_price = i, p_low
            # 检查是否突破阈值，形成 peak
            elif moved_up(last_ext_price, p_high, i) and (i - last_ext_idx) >= min_bars:
                pivots[i] = 1
                points[i] = p_high
                last_ext_idx, last_ext_price, last_type = i, p_high, "peak"

    # 保存结果到 df
    df["pivot"] = pivots
    df["zigzagpoint"] = points

    return df


def calculate_dmi(data, N=14, M=6):
    required_columns = ["high", "low", "close"]
    if not all(col in data.columns for col in required_columns):
        raise ValueError("DataFrame must contain 'high', 'low', and 'close' columns")

    tr1 = data["high"] - data["low"]
    tr2 = abs(data["high"] - data["close"].shift(1))
    tr3 = abs(data["low"] - data["close"].shift(1))
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    mtr = tr.rolling(window=N).sum()

    hd = data["high"] - data["high"].shift(1)
    ld = data["low"].shift(1) - data["low"]

    dmp_condition = (hd > 0) & (hd > ld)
    dmm_condition = (ld > 0) & (ld > hd)

    dmp_series = np.where(dmp_condition, hd, 0)
    dmm_series = np.where(dmm_condition, ld, 0)

    dmp = pd.Series(dmp_series, index=data.index).rolling(window=N).sum()
    dmm = pd.Series(dmm_series, index=data.index).rolling(window=N).sum()

    data["pdi"] = dmp * 100 / mtr
    data["mdi"] = dmm * 100 / mtr

    adx_series = abs(data["mdi"] - data["pdi"]) / (data["mdi"] + data["pdi"]) * 100
    data["adx"] = adx_series.rolling(window=M).mean()

    data["adxr"] = (data["adx"] + data["adx"].shift(M)) / 2

    return data


def calculate_dkx(data):
    m = 10
    data["mid"] = (3 * data["close"] + data["high"] + data["low"] + data["open"]) / 6

    weights = list(range(20, -1, -1))  # [20, 19, ..., 1, 0]
    data["dkx"] = sum(data["mid"].shift(i) * w for i, w in enumerate(weights)) / 210
    data["madkx"] = data["dkx"].rolling(window=m).mean()
    data = data.drop(columns=["mid"])
    return data


# 定义 SMMA（平滑移动平均线）函数
def smma(series, period):
    series = series.map(float)
    smma_values = [series.iloc[0]]  # 初始化第一个值为第一个数据
    alpha = 1 / period  # 平滑因子
    for price in series.iloc[1:]:
        smma_values.append(smma_values[-1] + alpha * (price - smma_values[-1]))
    return pd.Series(smma_values, index=series.index)


# 计算鳄鱼线，包括蓝线 (Jaw)、红线 (Teeth)、绿线 (Lips)。
def calculate_alligator(data):
    data = data.copy()
    data["Jaw"] = smma(data["close"], 13).shift(8)
    data["Teeth"] = smma(data["close"], 8).shift(5)
    data["Lips"] = smma(data["close"], 5).shift(3)
    return data


# 找到向上分形（Fractal Up）和向下分形（Fractal Down）。
def calculate_fractals(data):
    data["Fractal_Up"] = np.nan
    data["Fractal_Down"] = np.nan

    # 计算向上分形（high 大于前后两天的 high）
    data["Fractal_Up"] = data["high"][
        (data["high"] > data["high"].shift(1))
        & (data["high"] > data["high"].shift(2))
        & (data["high"] > data["high"].shift(-1))
        & (data["high"] > data["high"].shift(-2))
    ]

    # 计算向下分形（low 小于前后两天的 low）
    data["Fractal_Down"] = data["low"][
        (data["low"] < data["low"].shift(1))
        & (data["low"] < data["low"].shift(2))
        & (data["low"] < data["low"].shift(-1))
        & (data["low"] < data["low"].shift(-2))
    ]

    return data


# 计算 AO（Awesome Oscillator）动量指标。
def calculate_ao(data, fast_period=5, slow_period=34):
    data["Median_Price"] = (data["high"] + data["low"]) / 2
    data["Fast_SMA"] = data["Median_Price"].rolling(window=fast_period).mean()
    data["Slow_SMA"] = data["Median_Price"].rolling(window=slow_period).mean()
    data["AO"] = data["Fast_SMA"] - data["Slow_SMA"]

    data.drop(columns=["Median_Price", "Fast_SMA", "Slow_SMA"], inplace=True)

    return data


# 计算 AC（Accelerator Oscillator，加速震荡指标）。
def calculate_ac(data, ao_fast_period=5):
    data["SMA_AO"] = data["AO"].rolling(window=ao_fast_period).mean()
    data["AC"] = data["SMA_AO"] - data["AO"]

    data.drop(columns=["SMA_AO"], inplace=True)

    return data


def check_alligator_up(data, n=5):
    """
    判断最近 n 天是否满足收盘价 > 绿线 > 红线 > 蓝线。
    :param data: 包含 'close', 'Jaw', 'Teeth', 'Lips' 列的 DataFrame
    :param n: 检查的天数
    :return: True 或 False
    """
    recent_data = data.iloc[-n:]  # 获取最近 n 天的数据
    condition = (
        (recent_data["close"] > recent_data["Lips"])
        & (recent_data["Lips"] > recent_data["Teeth"])
        & (recent_data["Teeth"] > recent_data["Jaw"])
    )
    return condition.all()  # 如果所有天都满足条件，返回 True


def check_alligator_down(data, n=5):
    """
    判断最近 n 天是否满足收盘价 < 蓝线 < 红线 < 绿线。
    :param data: 包含 'close', 'Jaw', 'Teeth', 'Lips' 列的 DataFrame
    :param n: 检查的天数
    :return: True 或 False
    """
    recent_data = data.iloc[-n:]  # 获取最近 n 天的数据
    condition = (
        (recent_data["close"] < recent_data["Jaw"])
        & (recent_data["Jaw"] < recent_data["Teeth"])
        & (recent_data["Teeth"] < recent_data["Lips"])
    )
    return condition.all()  # 如果所有天都满足条件，返回 True


# 检查最近 3 天是否突破最近的向上分形
def check_fractal_up(data):
    """
    检查最近 3 天的任意一天是否突破了前面最近的向上分形。
    :param data: 包含 'close' 和 'Fractal_Up' 列的 DataFrame
    :return: True 或 False
    """
    # 找到最近的向上分形（忽略最后 3 天的分形）
    fractal_up = data["Fractal_Up"].dropna()

    # 确保索引是整数索引（如果是时间索引，需要先重置索引）
    if not isinstance(fractal_up.index, pd.RangeIndex):
        fractal_up = fractal_up.reset_index(drop=True)

    # 获取最近的向上分形值（排除最后 3 天）
    recent_fractal_up = fractal_up[fractal_up.index < len(data) - 3]
    if recent_fractal_up.empty:  # 如果没有找到向上分形，返回 False
        return False

    last_fractal_value = recent_fractal_up.iloc[-1]  # 最近的向上分形值

    # 检查最近 3 天的任意一天是否突破
    recent_close = data["close"].iloc[-3:]  # 最近 3 天的收盘价
    return (recent_close > last_fractal_value).any()  # 如果任意一天突破，返回 True


def check_ao_buy_signals(data):
    """
    检查最近三天内是否满足 AO 的买入信号之一：
    1. 零轴交叉。
    2. 双峰信号。
    3. 碟形信号。

    参数：
    - data: pd.DataFrame，必须包含一个 'AO' 列。

    返回：
    - bool: 如果满足任意一个买入条件，返回 True；否则返回 False。
    """
    # 确保数据包含 AO 列
    if "AO" not in data.columns:
        raise ValueError("DataFrame 必须包含 'AO' 列。")

    # 获取最近 3 天的数据
    recent = data.iloc[-3:]

    # 检查零轴交叉信号
    for i in range(1, len(recent)):
        if recent["AO"].iloc[i - 1] < 0 and recent["AO"].iloc[i] > 0:
            return True

    # 检查双峰信号
    if len(data) >= 5:  # 至少需要历史数据支持双峰信号
        for i in range(2, len(recent)):
            # 最近 3 天是否满足条件
            if (
                data["AO"].iloc[-5] < 0  # AO 在零轴下
                and data["AO"].iloc[-4] < data["AO"].iloc[-5]  # 第一个峰值较低
                and data["AO"].iloc[-2] > data["AO"].iloc[-3]  # 第二个峰值较高
                and data["AO"].iloc[-1] > data["AO"].iloc[-2]  # 跟随一个绿色柱状线
            ):
                return True

    # 检查碟形信号
    if len(data) >= 3:  # 至少需要历史数据支持碟形信号
        for i in range(2, len(recent)):
            if (
                data["AO"].iloc[-3] > 0  # AO 在零轴上方
                and data["AO"].iloc[-2] < data["AO"].iloc[-3]  # 连续两个红色柱状线
                and data["AO"].iloc[-1] > data["AO"].iloc[-2]  # 跟随一个绿色柱状线
            ):
                return True

    # 如果没有满足任意条件，则返回 False
    return False


# ==========
# K线形态检测函数
# ==========
def is_bullish_pinbar(row):
    """判断单根K线是否为看涨Pinbar"""
    high, low, open_, close = row["high"], row["low"], row["open"], row["close"]
    amplitude = (high - low) / low * 100
    if amplitude <= 4:
        return False
    upper_shadow = high - max(open_, close)
    lower_shadow = min(open_, close) - low
    body = abs(open_ - close)
    total = high - low
    return (
        lower_shadow > 2 * body
        and upper_shadow <= 0.5 * body
        and body < 0.3 * total
        and close >= open_
    )


def is_bullish_engulfing(prev, curr):
    """判断两天是否为看涨吞没"""
    amplitude = (curr["high"] - curr["low"]) / curr["low"] * 100
    if amplitude <= 3:
        return False
    return (
        prev["close"] < prev["open"]
        and curr["close"] > curr["open"]
        and curr["open"] < prev["close"]
        and curr["close"] > prev["open"]
        and curr["high"] > prev["high"]
        and curr["low"] < prev["low"]
    )


def is_morning_star(first, second, third):
    """判断三天是否为启明星"""
    return (
        first["close"] < first["open"]
        and second["close"] < first["close"]
        and third["close"] > third["open"]
        and third["close"] > first["open"]
    )


def check_bullish_patterns(data: pd.DataFrame) -> bool:
    """判断最近5天内是否出现看涨反转形态"""
    if len(data) < 2:
        return False
    n = len(data)
    for i in range(n):
        if is_bullish_pinbar(data.iloc[i]):
            return True
    for i in range(1, n):
        if is_bullish_engulfing(data.iloc[i - 1], data.iloc[i]):
            return True
    for i in range(2, n):
        if is_morning_star(data.iloc[i - 2], data.iloc[i - 1], data.iloc[i]):
            return True
    return False
