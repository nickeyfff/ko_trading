import os
import tempfile

import pandas as pd

from common import download_file, get_logger
from database.base import DuckDBBase

logger = get_logger(__name__)


# 基于 DuckDBBase 的 IndexTable 类
class Index(DuckDBBase):
    def __init__(self):
        super().__init__()
        self.table_name = "raw_index_constituent"
        self._create_index_table()

    def _create_index_table(self):
        """创建 index_table 表"""
        columns = {
            "index_name": "varchar",
            "name": "varchar",
            "symbol": "varchar",
        }
        self.create_table(self.table_name, columns)


class CSI(Index):
    def query(self, csi_name) -> pd.DataFrame:
        """查询 index_table 表，返回 DataFrame"""
        conditions = {"index_name": csi_name}
        return self.select(table_name=self.table_name, conditions=conditions)

    def store_csi_xls(self, xls_file: str):
        """解析 Excel 文件并导入数据到 csi 表"""
        # Excel 列名到数据库列名的映射
        xls_column_mapping = {
            "指数英文名称Index Name(Eng)": "index_name",
            "成份券代码Constituent Code": "code",
            "成份券名称Constituent Name": "name",
            "交易所英文名称Exchange(Eng)": "exchange",
        }
        # 交易所名称到简写映射
        exchange_mapping = {
            "Shenzhen Stock Exchange": "SZ",
            "Shanghai Stock Exchange": "SH",
            "Beijing Stock Exchange": "BJ",
        }

        try:
            # 读取 Excel 文件
            df = pd.read_excel(xls_file, dtype=str)
            # 保留需要的列
            columns_to_keep = list(xls_column_mapping.keys())
            data = df[columns_to_keep].copy()
            # 重命名列
            data.rename(columns=xls_column_mapping, inplace=True)
            # 替换交易所名称
            data["exchange"] = data["exchange"].replace(exchange_mapping)
            # 去除 index_name 中的空格
            data["index_name"] = data["index_name"].str.replace(" ", "", regex=True)
            # 生成 symbol 列（code.exchange）
            data["symbol"] = (data["exchange"].str.lower()).str.cat(data["code"])

            # 避免重复数据
            conditions = {"index_name": data["index_name"].iloc[0]}
            self.delete(self.table_name, conditions)

            column_order = ["index_name", "name", "symbol"]
            data = data[column_order].copy()
            # 插入 DataFrame 数据
            inserted_rows = self.insert_dataframe(self.table_name, data)

            filename = os.path.basename(xls_file)
            logger.info(
                f"从 {filename} 导入 {inserted_rows} 条数据到 {self.table_name} 表"
            )
        except Exception as e:
            logger.error(f"导入 {xls_file} 失败: {e}")
            raise


csi = CSI()


def update_index():
    """Update CSI, SP500, and GGT IndexTable by downloading and processing index files."""

    with tempfile.TemporaryDirectory() as temp_dir:
        os.makedirs(temp_dir, exist_ok=True)

        # Unified configuration for all index files
        index_configs = [
            {
                "name": "ChinaA",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/930903cons.xls",
                "output_file": "930903cons.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
            {
                "name": "沪深300",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/000300cons.xls",
                "output_file": "000300cons.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
            {
                "name": "中证500",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/000905cons.xls",
                "output_file": "000905cons.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
            {
                "name": "中证1000",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/000852cons.xls",
                "output_file": "000852cons.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
            {
                "name": "中证2000",
                "url": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/cons/932000cons.xls",
                "output_file": "932000cons.xls",
                "processor": csi.store_csi_xls,
                "headers": None,
            },
        ]

        # Download and process index file sequentially
        for config in index_configs:
            output_path = os.path.join(temp_dir, config["output_file"])
            if download_file(config["url"], output_path, headers=config["headers"]):
                try:
                    config["processor"](output_path)
                except Exception as e:
                    print(f"Error processing {config['name']}: {e}")
            else:
                print(f"Failed to download {config['name']}")


if __name__ == "__main__":
    update_index()
