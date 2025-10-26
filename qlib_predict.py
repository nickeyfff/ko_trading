import os

import pandas as pd
import qlib
from qlib.constant import REG_CN
from qlib.utils import init_instance_by_config

# 用户配置
TOP_N_STOCKS = 50
OUTPUT_FILE = "stock_picks.csv"
PROVIDER_URI = os.getenv("QLIB_PROVIDER_URI")


def get_daily_stock_picks():
    qlib.init(
        provider_uri=PROVIDER_URI,
        region=REG_CN,
        exp_manager={
            "class": "MLflowExpManager",
            "module_path": "qlib.workflow.expm",
            "kwargs": {
                "uri": "./mlruns",
                "default_exp_name": "Experiment",
            },
        },
    )

    today = pd.Timestamp.now().normalize()
    validation_start_time = today - pd.DateOffset(months=3)

    handler_config = {
        "class": "Alpha158",
        "module_path": "qlib.contrib.data.handler",
        "kwargs": {
            "start_time": "2017-01-01",
            "end_time": today,
            "fit_start_time": "2017-01-01",
            "fit_end_time": today,
            "instruments": "all",
        },
    }

    dataset_config = {
        "class": "DatasetH",
        "module_path": "qlib.data.dataset",
        "kwargs": {
            "handler": handler_config,
            "segments": {
                "train": ("2017-01-01", validation_start_time),
                "valid": (validation_start_time, today),
            },
        },
    }

    model_config = {
        "class": "LGBModel",
        "module_path": "qlib.contrib.model.gbdt",
        "kwargs": {
            "loss": "mse",
            "colsample_bytree": 0.8879,
            "learning_rate": 0.0421,
            "subsample": 0.8789,
            "n_estimators": 200,
            "max_depth": 8,
            "num_leaves": 210,
            "early_stopping_rounds": 50,
        },
    }

    print("正在初始化 Dataset...")
    dataset = init_instance_by_config(dataset_config)

    print("正在初始化 Model...")
    model = init_instance_by_config(model_config)

    print("模型正在使用所有历史数据进行训练...")
    model.fit(dataset)

    print("模型训练完成，正在生成预测分数...")
    pred_train = model.predict(dataset, segment="train")
    pred_valid = model.predict(dataset, segment="valid")
    prediction = pd.concat([pred_train, pred_valid])

    print("正在提取最新交易日的选股排名...")
    latest_date = prediction.index.get_level_values("datetime").max()

    daily_prediction = prediction.loc[latest_date]

    if isinstance(daily_prediction, pd.Series):
        daily_prediction = daily_prediction.to_frame("score")

    daily_ranking = daily_prediction.sort_values(by="score", ascending=False)

    top_stocks = daily_ranking.head(TOP_N_STOCKS)

    print("-" * 50)
    print(
        f"模型为下一个交易日选出的排名前 {TOP_N_STOCKS} 股票 (基于 {latest_date.date()} 的数据):"
    )
    print(top_stocks)
    print("-" * 50)

    top_stocks.to_csv(OUTPUT_FILE)
    print(f"选股结果已保存到: {OUTPUT_FILE}")


if __name__ == "__main__":
    get_daily_stock_picks()
