import argparse
import os

import pandas as pd

from database.base import DuckDBBase


class History(DuckDBBase):
    def __init__(self):
        super().__init__()
        self.table_name = "history"
        self._create_history_table()

    def _create_history_table(self):
        """创建 history 表"""
        columns = {
            "code": "TEXT",
            "name": "TEXT",
            "pl": "FLOAT",
            "broker": "TEXT",
        }
        self.create_table(self.table_name, columns)

    def store_csv(self, csv_file):
        """从 CSV 文件导入数据到 history 表"""
        try:
            df = pd.read_csv(csv_file, dtype=str)
            inserted_rows = self.insert_dataframe(self.table_name, df)
            return inserted_rows
        except Exception as e:
            raise Exception(f"导入 {csv_file} 失败: {e}")

    def query(self):
        """查询 history 表，返回 DataFrame"""
        return self.select(table_name=self.table_name)


history = History()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="导入历史交易股票信息")

    parser.add_argument(
        "-i",
        required=True,
        help="历史交易股票的 csv 文件路径",
    )

    # 解析命令行参数
    args = parser.parse_args()

    if args.i:
        if not os.path.isfile(args.i):
            print(f"File '{args.i}' does not exist. Skipping...")
        try:
            history.store_csv(args.i)
        except Exception as e:
            print(f"Error processing file '{args.i}': {e}")
    else:
        print("Path is required for loading data.")
