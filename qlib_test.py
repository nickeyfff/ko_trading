import os

import qlib
from qlib.constant import REG_CN
from qlib.utils import init_instance_by_config
from qlib.workflow import R
from qlib.workflow.record_temp import PortAnaRecord, SignalRecord

tracking_uri = "./mlruns"
provider_uri = os.getenv("QLIB_PROVIDER_URI")

qlib.init(provider_uri=provider_uri, region=REG_CN)

# 定义 Data Handler 配置 (使用 Alpha158)
handler_config = {
    "class": "Alpha158",
    "module_path": "qlib.contrib.data.handler",
    "kwargs": {
        "start_time": "2017-01-01",
        "end_time": "2023-12-31",
        "fit_start_time": "2017-01-01",
        "fit_end_time": "2020-12-31",
        "instruments": "all",
    },
}

# 定义 Dataset 配置
dataset_config = {
    "class": "DatasetH",
    "module_path": "qlib.data.dataset",
    "kwargs": {
        "handler": handler_config,
        "segments": {
            "train": ("2017-01-01", "2020-12-31"),
            "valid": ("2021-01-01", "2021-12-31"),
            "test": ("2022-01-01", "2023-12-31"),
        },
    },
}

# 定义模型配置 (LGBModel)
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
    },
}

# 实例化 Dataset 和 Model
dataset = init_instance_by_config(dataset_config)
model = init_instance_by_config(model_config)

# 训练、预测和回测
with R.start(uri=tracking_uri, experiment_name="lgbm_alpha158"):
    print("Training model...")
    model.fit(dataset)

    print("Making predictions...")
    prediction = model.predict(dataset)
    R.save_objects(prediction=prediction)

    # 显式地将 model 和 dataset 传递给策略
    port_analysis_config = {
        "strategy": {
            "class": "TopkDropoutStrategy",
            "module_path": "qlib.contrib.strategy",
            "kwargs": {
                # 显式传递 model 和 dataset
                "model": model,
                "dataset": dataset,
                "topk": 50,
                "n_drop": 5,
            },
        },
        "backtest": {
            "start_time": "2022-01-01",
            "end_time": "2023-12-31",
            "account": 100000000,
            "benchmark": "SH000300",
            "exchange_kwargs": {
                "limit_threshold": 0.095,
                "deal_price": "close",
                "open_cost": 0.0005,
                "close_cost": 0.0015,
            },
        },
    }

    recorder = R.get_recorder()

    # 记录信号和投资组合分析结果
    print("Generating signal record...")
    rec_signal = SignalRecord(model=model, dataset=dataset, recorder=recorder)
    rec_signal.generate(segment="test")

    print("Generating portfolio analysis record...")
    rec_port = PortAnaRecord(config=port_analysis_config, recorder=recorder)
    rec_port.generate(segment="test")

print("Workflow finished successfully.")
