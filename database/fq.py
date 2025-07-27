import pandas as pd

from .gbbq import gbbq
from .stock import stock


def fq(symbol):
    code = symbol[2:]
    xdxr_data = gbbq.query(code=code)
    bfq_data = stock.query(symbol=symbol)
    return calculate_fq(bfq_data, xdxr_data)


def calculate_fq(bfq_data, xdxr_data, qfq=True):
    # 如果没有除权除息数据，直接返回 bfq_data，并添加默认列
    if xdxr_data is None or xdxr_data.empty:
        bfq_data["qfq_adj"] = 1
        bfq_data["hfq_adj"] = 1
        bfq_data["pre_close"] = bfq_data["close"].shift(1)
        return bfq_data

    # 添加交易标识列
    bfq_data = bfq_data.assign(if_trade=1)
    xdxr_data = xdxr_data.assign(xdxr=1)
    data = pd.merge(xdxr_data, bfq_data, on="date", how="outer")
    data.drop(columns=["code"], inplace=True)

    temp_columns = ["fenhong", "peigu", "peigujia", "songzhuangu", "if_trade", "xdxr"]

    for column in temp_columns:
        data.fillna({column: 0}, inplace=True)

    data.ffill(inplace=True)

    # 计算复权前的前一日收盘价
    data["pre_close"] = (
        (data["close"].shift(1) * 10 - data["fenhong"])
        + (data["peigu"] * data["peigujia"])
    ) / (10 + data["peigu"] + data["songzhuangu"])

    # 计算前复权因子 (qfq_adj)
    data["qfq_adj"] = (
        (data["pre_close"].shift(-1) / data["close"]).fillna(1)[::-1].cumprod()
    )

    # 计算后复权因子 (hfq_adj)
    data["hfq_adj"] = (
        (data["close"] / data["pre_close"].shift(-1)).cumprod().shift(1).fillna(1)
    )

    if qfq:
        for col in ["open", "high", "low", "close", "pre_close"]:
            data[col] = data[col] * data["qfq_adj"]
            data[col] = round(data[col], 2)
        data.drop(["qfq_adj", "hfq_adj"], axis=1, inplace=True)

    data = data[data["if_trade"] == 1]
    data.drop(
        temp_columns,
        axis=1,
        inplace=True,
    )
    # 保留所需的列并返回
    return data
