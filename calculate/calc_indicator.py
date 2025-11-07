from datetime import timedelta
from functools import partial

import pandas as pd

from calculate.my_talib import (
    calculate_adx,
    calculate_atr,
    calculate_bbands,
    calculate_ma,
    calculate_ma_power_ratio,
    calculate_macd,
    calculate_mavol,
)
from common import batch_processor
from database import indicator, stock


def calculate(
    symbol: str, start_date: str, end_date: str, lookback_days: int
) -> pd.DataFrame:
    """
    计算指定股票在某个日期范围内的技术指标。
    """
    query_start_date = (
        pd.to_datetime(start_date) - timedelta(days=lookback_days * 1.5 + 80)
    ).strftime("%Y-%m-%d")

    data = stock.query(symbol=symbol, start_date=query_start_date, end_date=end_date)

    if data.empty:
        return pd.DataFrame()

    data = data.set_index("date").sort_index()

    ma = calculate_ma(data)
    # 指标计算逻辑不变
    indicators = pd.concat(
        [
            ma,
            calculate_ma_power_ratio(ma),
            calculate_atr(data),
            calculate_mavol(data),
            calculate_macd(data),
            calculate_adx(data),
            calculate_bbands(data),
        ],
        axis=1,
    )

    # 保留需要插入数据库的日期范围 [start_date, end_date]
    indicators = indicators.loc[start_date:end_date]
    indicators = indicators.reset_index().rename(columns={"index": "date"})
    indicators["symbol"] = symbol

    indicators = indicators.copy()
    numeric_cols = indicators.select_dtypes(include="number").columns
    indicators[numeric_cols] = indicators[numeric_cols].round(2)

    return indicators.melt(
        id_vars=["date", "symbol"],
        var_name="indicator",
        value_name="value",
    ).dropna(subset=["value"])


def run_indicator_calculate(
    symbols: list[str],
    chunk_size: int = 500,
    max_workers: int = 8,
    lookback_days: int = 300,
):
    """
    执行指标计算和更新。
    """

    def execute(symbols_to_process, start, end):
        if not symbols_to_process:
            print("无需处理任何股票，跳过。")
            return

        print(f"处理 {len(symbols_to_process)} 只股票, 日期范围: {start} - {end}")

        worker = partial(
            calculate, start_date=start, end_date=end, lookback_days=lookback_days
        )

        for i, results_list in enumerate(
            batch_processor(
                items=symbols_to_process,
                worker_func=worker,
                max_workers=max_workers,
                chunk_size=chunk_size,
            )
        ):
            try:
                print(
                    f"第 {i + 1} 批计算完成，合并 {len(results_list)} 个结果并准备入库..."
                )
                combined_df = pd.concat(results_list, ignore_index=True)
                print(f"正在将 {len(combined_df)} 条指标插入数据库...")
                indicator.insert(combined_df)
                print("✅ 插入成功。")
            except Exception as e:
                print(f"❌ 插入失败: {e}")

    print(f"\n{'=' * 50}\n开始技术指标计算和更新")
    latest_indicator_date = indicator.get_latest_date()
    is_full_init = latest_indicator_date is None
    start_date = (
        "1900-01-01"
        if is_full_init
        else (pd.to_datetime(latest_indicator_date) + timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )
    )
    latest_stock_date = stock.get_latest_date()

    if latest_stock_date is None:
        print(f"❌ 数据库无股票日线数据，任务退出\n{'=' * 50}\n")
        return

    end_date = latest_stock_date

    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        print(f"✅ 指标数据已是最新\n{'=' * 50}\n")
        return

    if is_full_init:
        print("数据库无指标，将进行全量初始化。")

    execute(
        symbols_to_process=symbols,
        start=start_date,
        end=end_date,
    )

    if not is_full_init:
        xdxr_symbols = stock.list_stocks_with_xdxr(start_date=start_date)
        symbols_to_refresh = list(set(xdxr_symbols) & set(symbols))

        if symbols_to_refresh:
            print(f"\n近期有 {len(symbols_to_refresh)} 只股票除权除息")
            print(f"\n删除 {len(symbols_to_refresh)} 只股票的历史指标")
            indicator.delete_symbols(symbols_to_refresh)

            execute(
                symbols_to_process=symbols_to_refresh,
                start="1900-01-01",
                end=end_date,
            )

    print(f"🎉 指标更新完成\n{'=' * 50}\n")
